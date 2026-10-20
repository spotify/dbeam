/*-
 * -\-\-
 * DBeam Parquet
 * --
 * Copyright (C) 2016 - 2025 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.dbeam.parquet;

import com.google.common.base.Preconditions;
import com.spotify.dbeam.args.JdbcExportArgs;
import com.spotify.dbeam.avro.JdbcMetering;
import com.spotify.dbeam.beam.BeamHelper;
import com.spotify.dbeam.beam.MetricsHelper;
import com.spotify.dbeam.jobs.ExceptionHandling;
import com.spotify.dbeam.jobs.FailedValidationException;
import com.spotify.dbeam.options.DBeamPipelineOptions;
import com.spotify.dbeam.options.JdbcExportArgsFactory;
import com.spotify.dbeam.options.JdbcExportPipelineOptions;
import com.spotify.dbeam.options.JobNameConfiguration;
import com.spotify.dbeam.options.OutputOptions;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcParquetJob {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcParquetJob.class);

  private final PipelineOptions pipelineOptions;
  private final Pipeline pipeline;
  private final JdbcExportArgs jdbcExportArgs;
  private final String output;
  private final boolean dataOnly;
  private final long minRows;

  public JdbcParquetJob(
      final PipelineOptions pipelineOptions,
      final Pipeline pipeline,
      final JdbcExportArgs jdbcExportArgs,
      final String output,
      final boolean dataOnly,
      final long minRows) {
    this.pipelineOptions = pipelineOptions;
    this.pipeline = pipeline;
    this.jdbcExportArgs = jdbcExportArgs;
    this.output = output;
    this.dataOnly = dataOnly;
    this.minRows = minRows;
    Preconditions.checkArgument(
        this.output != null && this.output.length() > 0, "'output' must be defined");
  }

  public static JdbcParquetJob create(final PipelineOptions pipelineOptions, final String output)
      throws IOException, ClassNotFoundException {
    pipelineOptions.as(DirectOptions.class).setBlockOnRun(false);
    return new JdbcParquetJob(
        pipelineOptions,
        Pipeline.create(pipelineOptions),
        JdbcExportArgsFactory.fromPipelineOptions(pipelineOptions),
        output,
        pipelineOptions.as(OutputOptions.class).getDataOnly(),
        pipelineOptions.as(JdbcExportPipelineOptions.class).getMinRows());
  }

  public static JdbcParquetJob create(final PipelineOptions pipelineOptions)
      throws IOException, ClassNotFoundException {
    return create(pipelineOptions, pipelineOptions.as(OutputOptions.class).getOutput());
  }

  public static JdbcParquetJob create(final String[] cmdLineArgs)
      throws IOException, ClassNotFoundException {
    return create(buildPipelineOptions(cmdLineArgs));
  }

  public static PipelineOptions buildPipelineOptions(final String[] cmdLineArgs) {
    PipelineOptionsFactory.register(JdbcExportPipelineOptions.class);
    PipelineOptionsFactory.register(OutputOptions.class);
    PipelineOptionsFactory.register(ParquetPipelineOptions.class);
    return PipelineOptionsFactory.fromArgs(cmdLineArgs).withValidation().create();
  }

  private void configureVersion() {
    final String dbeamVersion = this.getClass().getPackage().getImplementationVersion();
    LOGGER.info(
        "{} {} version {}",
        this.getClass().getPackage().getImplementationTitle(),
        this.getClass().getSimpleName(),
        dbeamVersion);
    pipelineOptions.as(DBeamPipelineOptions.class).setDBeamVersion(dbeamVersion);
  }

  public void prepareExport() throws Exception {
    configureVersion();
    final List<String> queries;
    final MessageType generatedSchema;
    final Schema avroSchema;
    final String arrayMode = jdbcExportArgs.jdbcAvroOptions().arrayMode();
    if (Boolean.TRUE.equals(jdbcExportArgs.jdbcAvroOptions().nullableArrayItems())) {
      LOGGER.warn(
          "--nullableArrayItems has no effect on Parquet exports: "
              + "Parquet list elements are always optional");
    }
    try (Connection connection = jdbcExportArgs.createConnection()) {
      final String schemaFilePath =
          pipelineOptions.as(ParquetPipelineOptions.class).getParquetSchemaFilePath();
      final java.util.Optional<MessageType> inputSchema =
          BeamJdbcParquetSchema.parseOptionalInputParquetSchemaFile(schemaFilePath);
      if (inputSchema.isPresent()) {
        final long startTime = System.nanoTime();
        generatedSchema = inputSchema.get();
        validateInputSchema(connection, generatedSchema, arrayMode);
        avroSchema =
            footerAvroSchema(() -> deriveFooterAvroSchema(generatedSchema, connection));
        BeamJdbcParquetSchema.exposeSchemaMetrics(pipeline, System.nanoTime() - startTime);
      } else {
        // Query DB once for both Parquet and Avro schemas
        final long startTime = System.nanoTime();
        try (Statement stmt = connection.createStatement()) {
          final ResultSet rs =
              stmt.executeQuery(jdbcExportArgs.queryBuilderArgs().sqlQueryWithLimitOne());
          rs.next();
          generatedSchema =
              JdbcParquetSchema.createParquetSchema(
                  rs,
                  jdbcExportArgs.avroSchemaName(),
                  jdbcExportArgs.useAvroLogicalTypes(),
                  jdbcExportArgs.useTimestampMicros(),
                  arrayMode);
          final MessageType writeSchema = generatedSchema;
          avroSchema =
              footerAvroSchema(
                  () -> deriveFooterAvroSchema(writeSchema, connection));
        }
        BeamJdbcParquetSchema.exposeSchemaMetrics(pipeline, System.nanoTime() - startTime);
      }
      queries = jdbcExportArgs.queryBuilderArgs().buildQueries(connection);

      final String tableName = pipelineOptions.as(DBeamPipelineOptions.class).getTable();
      JobNameConfiguration.configureJobName(
          pipeline.getOptions(), connection.getCatalog(), tableName);
    }
    if (!this.dataOnly) {
      BeamHelper.saveStringOnSubPath(output, "/_PARQUET_SCHEMA.json", generatedSchema.toString());
      if (avroSchema != null) {
        BeamHelper.saveStringOnSubPath(output, "/_AVRO_SCHEMA.avsc", avroSchema.toString(true));
      }
      for (int i = 0; i < queries.size(); i++) {
        BeamHelper.saveStringOnSubPath(
            this.output, String.format("/_queries/query_%d.sql", i), queries.get(i));
      }
    }
    LOGGER.info("Running queries: {}", queries.toString());

    final String parquetCodec = resolveParquetCodec(pipelineOptions);
    final ParquetPipelineOptions parquetOptions = pipelineOptions.as(ParquetPipelineOptions.class);
    final JdbcParquetArgs parquetArgs =
        JdbcParquetArgs.create(
            jdbcExportArgs.jdbcAvroOptions().jdbcConnectionConfiguration(),
            jdbcExportArgs.jdbcAvroOptions().fetchSize(),
            parquetCodec,
            parquetOptions.getRowGroupSize(),
            parquetOptions.getPageSize(),
            jdbcExportArgs.jdbcAvroOptions().preCommand(),
            arrayMode);

    pipeline
        .apply("JdbcQueries", Create.of(queries))
        .apply(
            "JdbcParquetSave",
            JdbcParquetIO.createWrite(
                output,
                ".parquet",
                generatedSchema,
                parquetArgs,
                avroSchema == null ? null : avroSchema.toString()));
  }

  private void checkMetrics(PipelineResult pipelineResult) throws FailedValidationException {
    final Map<String, Long> metrics = MetricsHelper.getMetrics(pipelineResult);
    if (!this.dataOnly) {
      BeamHelper.saveMetrics(metrics, output);
    }
    final Long recordCount = metrics.getOrDefault(JdbcMetering.RECORD_COUNT_METRIC_NAME, 0L);
    if (recordCount < this.minRows) {
      throw new FailedValidationException(
          String.format(
              "Unexpected number of rows in the output: got %d, expecting at least %d",
              recordCount, this.minRows));
    }
  }

  public PipelineResult runAndWait() {
    return BeamHelper.waitUntilDone(this.pipeline.run(), jdbcExportArgs.exportTimeout());
  }

  public PipelineResult runExport() throws Exception {
    prepareExport();
    final PipelineResult pipelineResult = runAndWait();
    checkMetrics(pipelineResult);
    return pipelineResult;
  }

  interface AvroSchemaSupplier {
    Schema get() throws Exception;
  }

  /**
   * The Avro schema only feeds the optional {@code parquet.avro.schema} footer key, so failing to
   * build it (e.g. a schema file using INT96 or names that are not valid Avro names) should not
   * fail a Parquet export that is otherwise valid.
   */
  static Schema footerAvroSchema(final AvroSchemaSupplier supplier) {
    try {
      return supplier.get();
    } catch (Exception e) {
      LOGGER.warn(
          "Could not generate Avro schema; writing Parquet without parquet.avro.schema footer", e);
      return null;
    }
  }

  /**
   * Fail fast if the schema from --parquetSchemaFilePath does not match what the writers emit for
   * the JDBC result set. See {@link ParquetSchemaValidator} for the rules. This is the only
   * metadata query on the schema-file path; the footer Avro schema is derived from the schema
   * file itself.
   */
  private void validateInputSchema(
      final Connection connection, final MessageType inputSchema, final String arrayMode)
      throws Exception {
    try (Statement stmt = connection.createStatement()) {
      final ResultSet rs =
          stmt.executeQuery(jdbcExportArgs.queryBuilderArgs().sqlQueryWithLimitOne());
      rs.next();
      final boolean micros = jdbcExportArgs.useTimestampMicros();
      final MessageType derived =
          JdbcParquetSchema.createParquetSchema(
              rs, jdbcExportArgs.avroSchemaName(), false, micros, arrayMode);
      final MessageType derivedLogical =
          JdbcParquetSchema.createParquetSchema(
              rs, jdbcExportArgs.avroSchemaName(), true, micros, arrayMode);
      ParquetSchemaValidator.validate(
          inputSchema,
          derived,
          derivedLogical,
          jdbcExportArgs.useAvroLogicalTypes() ? derivedLogical : derived);
    }
  }

  /**
   * The footer Avro schema is derived from the Parquet schema used for writing, so it always
   * describes the actual columns (see {@link ParquetAvroSchema}).
   */
  private Schema deriveFooterAvroSchema(final MessageType schema, final Connection connection)
      throws SQLException {
    final String dbUrl = connection.getMetaData().getURL();
    final String avroDoc =
        jdbcExportArgs
            .avroDoc()
            .orElseGet(() -> String.format("Generate schema from JDBC ResultSet from %s", dbUrl));
    final Schema avroSchema =
        ParquetAvroSchema.toAvroSchema(schema, jdbcExportArgs.avroSchemaNamespace(), avroDoc);
    LOGGER.info("Footer Avro schema derived from Parquet schema: {}", avroSchema);
    return avroSchema;
  }

  public Pipeline getPipeline() {
    return pipeline;
  }

  public JdbcExportArgs getJdbcExportArgs() {
    return jdbcExportArgs;
  }

  public String getOutput() {
    return output;
  }

  static String resolveParquetCodec(final PipelineOptions options) {
    final String parquetCodec = options.as(ParquetPipelineOptions.class).getParquetCodec();
    if (parquetCodec != null && !parquetCodec.isEmpty()) {
      return parquetCodec;
    }
    return mapAvroCodecToParquetCodec(options.as(JdbcExportPipelineOptions.class).getAvroCodec());
  }

  static String mapAvroCodecToParquetCodec(final String avroCodec) {
    if (avroCodec == null) {
      return "snappy";
    }
    if (avroCodec.equals("snappy")) {
      return "snappy";
    } else if (avroCodec.startsWith("deflate")) {
      return "gzip";
    } else if (avroCodec.startsWith("zstandard")) {
      return "zstd";
    }
    return avroCodec;
  }

  public static void main(String[] cmdLineArgs) {
    try {
      JdbcParquetJob.create(cmdLineArgs).runExport();
    } catch (Exception e) {
      ExceptionHandling.handleException(e);
    }
  }
}
