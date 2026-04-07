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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableMap;
import java.nio.channels.WritableByteChannel;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.DynamicFileDestinations;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.ShardNameTemplate;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JdbcParquetIO {

  private static final String DEFAULT_SHARD_TEMPLATE = ShardNameTemplate.INDEX_OF_MAX;

  public static final String PARQUET_AVRO_SCHEMA_KEY = "parquet.avro.schema";

  public static PTransform<PCollection<String>, WriteFilesResult<Void>> createWrite(
      final String filenamePrefix,
      final String filenameSuffix,
      final MessageType schema,
      final JdbcParquetArgs jdbcParquetArgs,
      final String avroSchemaJson) {
    final ValueProvider<ResourceId> prefixProvider =
        StaticValueProvider.of(
            FileBasedSink.convertToFileResourceIfPossible(
                filenamePrefix.replaceAll("/+$", "") + "/part"));
    final FileBasedSink.FilenamePolicy filenamePolicy =
        DefaultFilenamePolicy.fromStandardParameters(
            prefixProvider, DEFAULT_SHARD_TEMPLATE, filenameSuffix, false);

    final FileBasedSink.DynamicDestinations<String, Void, String> destinations =
        DynamicFileDestinations.constant(filenamePolicy, SerializableFunctions.identity());

    // Store schema as String for serialization (MessageType is not Serializable)
    final String schemaString = schema.toString();
    final FileBasedSink<String, Void, String> sink =
        new JdbcParquetSink(
            prefixProvider, destinations, schemaString, jdbcParquetArgs, avroSchemaJson);
    return WriteFiles.to(sink);
  }

  static class JdbcParquetSink extends FileBasedSink<String, Void, String> {

    private static final long serialVersionUID = 937707428039L;
    private final String schemaString;
    private final JdbcParquetArgs jdbcParquetArgs;
    private final String avroSchemaJson;

    JdbcParquetSink(
        final ValueProvider<ResourceId> filenamePrefix,
        final DynamicDestinations<String, Void, String> destinations,
        final String schemaString,
        final JdbcParquetArgs jdbcParquetArgs,
        final String avroSchemaJson) {
      super(filenamePrefix, destinations);
      this.schemaString = schemaString;
      this.jdbcParquetArgs = jdbcParquetArgs;
      this.avroSchemaJson = avroSchemaJson;
    }

    @Override
    public WriteOperation<Void, String> createWriteOperation() {
      return new JdbcParquetWriteOperation(this, schemaString, jdbcParquetArgs, avroSchemaJson);
    }
  }

  private static class JdbcParquetWriteOperation
      extends FileBasedSink.WriteOperation<Void, String> {

    private static final long serialVersionUID = 305340251351L;
    private final String schemaString;
    private final JdbcParquetArgs jdbcParquetArgs;
    private final String avroSchemaJson;

    private JdbcParquetWriteOperation(
        final FileBasedSink<?, Void, String> sink,
        final String schemaString,
        final JdbcParquetArgs jdbcParquetArgs,
        final String avroSchemaJson) {
      super(sink);
      this.schemaString = schemaString;
      this.jdbcParquetArgs = jdbcParquetArgs;
      this.avroSchemaJson = avroSchemaJson;
    }

    @Override
    public FileBasedSink.Writer<Void, String> createWriter() {
      return new JdbcParquetWriter(this, schemaString, jdbcParquetArgs, avroSchemaJson);
    }
  }

  private static class JdbcParquetWriter extends FileBasedSink.Writer<Void, String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcParquetWriter.class);
    private final String schemaString;
    private final JdbcParquetArgs jdbcParquetArgs;
    private final String avroSchemaJson;
    private ParquetWriter<ResultSet> parquetWriter;
    private Connection connection;
    private JdbcParquetMetering metering;
    private ChannelOutputFile channelOutputFile;

    JdbcParquetWriter(
        FileBasedSink.WriteOperation<Void, String> writeOperation,
        String schemaString,
        JdbcParquetArgs jdbcParquetArgs,
        String avroSchemaJson) {
      super(writeOperation, MimeTypes.BINARY);
      this.schemaString = schemaString;
      this.jdbcParquetArgs = jdbcParquetArgs;
      this.avroSchemaJson = avroSchemaJson;
      this.metering = JdbcParquetMetering.create();
    }

    public Void getDestination() {
      return null;
    }

    @Override
    protected void prepareWrite(final WritableByteChannel channel) throws Exception {
      LOGGER.info("jdbcparquetio : Preparing write...");
      connection = jdbcParquetArgs.jdbcConnectionConfiguration().createConnection();

      final MessageType schema = MessageTypeParser.parseMessageType(schemaString);
      channelOutputFile = new ChannelOutputFile(channel);
      parquetWriter = new ResultSetParquetWriterBuilder(
              channelOutputFile, schema, avroSchemaJson, jdbcParquetArgs.arrayMode())
          .withCompressionCodec(jdbcParquetArgs.getCompressionCodecName())
          .withRowGroupSize(jdbcParquetArgs.rowGroupSize())
          .withPageSize(jdbcParquetArgs.pageSize())
          .withWriteMode(ParquetFileWriter.Mode.CREATE)
          .build();
      LOGGER.info("jdbcparquetio : Write prepared");
    }

    private ResultSet executeQuery(final String query) throws Exception {
      checkArgument(connection != null, "JDBC connection was not properly created");
      final PreparedStatement statement =
          connection.prepareStatement(
              query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      statement.setFetchSize(jdbcParquetArgs.fetchSize());
      if (jdbcParquetArgs.statementPreparator() != null) {
        jdbcParquetArgs.statementPreparator().setParameters(statement);
      }

      if (jdbcParquetArgs.preCommand() != null
          && !jdbcParquetArgs.preCommand().isEmpty()) {
        final Statement stmt = connection.createStatement();
        for (String command : jdbcParquetArgs.preCommand()) {
          stmt.execute(command);
        }
      }

      final long startTime = System.nanoTime();
      LOGGER.info(
          "jdbcparquetio : Executing query with fetchSize={} (this might take a few minutes) ...",
          statement.getFetchSize());
      final ResultSet resultSet = statement.executeQuery();
      this.metering.exposeExecuteQueryMs((System.nanoTime() - startTime) / 1000000L);
      checkArgument(resultSet != null, "JDBC resultSet was not properly created");
      return resultSet;
    }

    @Override
    public void write(final String query) throws Exception {
      checkArgument(parquetWriter != null, "ParquetWriter was not properly created");
      LOGGER.info("jdbcparquetio : Starting write...");
      try (ResultSet resultSet = executeQuery(query)) {
        metering.startWriteMeter();
        while (resultSet.next()) {
          parquetWriter.write(resultSet);
          this.metering.incrementRecordCount();
        }
        this.metering.exposeWriteElapsed();
        if (channelOutputFile != null) {
          this.metering.exposeWrittenBytes(channelOutputFile.getBytesWritten());
        }
      }
    }

    @Override
    protected void finishWrite() throws Exception {
      LOGGER.info("jdbcparquetio : Closing connection, flushing writer...");
      if (parquetWriter != null) {
        parquetWriter.close();
      }
      if (connection != null) {
        connection.close();
      }
      LOGGER.info("jdbcparquetio : Write finished");
    }
  }

  static class ResultSetParquetWriterBuilder
      extends ParquetWriter.Builder<ResultSet, ResultSetParquetWriterBuilder> {

    private final MessageType schema;
    private final String avroSchemaJson;
    private final String arrayMode;

    ResultSetParquetWriterBuilder(OutputFile outputFile, MessageType schema) {
      this(outputFile, schema, null, "typed_first_row");
    }

    ResultSetParquetWriterBuilder(
        OutputFile outputFile, MessageType schema, String avroSchemaJson) {
      this(outputFile, schema, avroSchemaJson, "typed_first_row");
    }

    ResultSetParquetWriterBuilder(
        OutputFile outputFile, MessageType schema, String avroSchemaJson, String arrayMode) {
      super(outputFile);
      this.schema = schema;
      this.avroSchemaJson = avroSchemaJson;
      this.arrayMode = arrayMode;
    }

    @Override
    protected ResultSetParquetWriterBuilder self() {
      return this;
    }

    @Override
    protected WriteSupport<ResultSet> getWriteSupport(Configuration conf) {
      return new ResultSetWriteSupport(schema, avroSchemaJson, arrayMode);
    }
  }

  static class ResultSetWriteSupport extends WriteSupport<ResultSet> {

    private final MessageType schema;
    private final String avroSchemaJson;
    private final String arrayMode;
    private JdbcParquetWriteSupport writeSupport;
    private RecordConsumer recordConsumer;
    private boolean initialized = false;

    ResultSetWriteSupport(MessageType schema, String avroSchemaJson, String arrayMode) {
      this.schema = schema;
      this.avroSchemaJson = avroSchemaJson;
      this.arrayMode = arrayMode;
    }

    @Override
    public WriteContext init(Configuration configuration) {
      final Map<String, String> extraMetadata;
      if (avroSchemaJson != null && !avroSchemaJson.isEmpty()) {
        extraMetadata = ImmutableMap.of(PARQUET_AVRO_SCHEMA_KEY, avroSchemaJson);
      } else {
        extraMetadata = Collections.emptyMap();
      }
      return new WriteContext(schema, extraMetadata);
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
      this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(ResultSet resultSet) {
      try {
        if (!initialized) {
          this.writeSupport = JdbcParquetWriteSupport.create(resultSet, schema, arrayMode);
          initialized = true;
        }
        writeSupport.writeRecord(recordConsumer, resultSet);
      } catch (Exception e) {
        throw new RuntimeException("Failed to write ResultSet row to Parquet", e);
      }
    }
  }
}
