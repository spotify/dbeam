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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;

import com.spotify.dbeam.DbTestHelper;
import com.spotify.dbeam.TestHelper;
import com.spotify.dbeam.jobs.FailedValidationException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class JdbcParquetJobTest {

  private static final String CONNECTION_URL =
      "jdbc:h2:mem:testparquet;MODE=PostgreSQL;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1";
  private static Path testDir;
  private static Path passwordPath;
  private static Path sqlPath;
  private static final LocalDateTime TS_MICROS_VALUE =
      LocalDateTime.of(2026, 8, 20, 19, 6, 1, 879_123_000);

  @BeforeAll
  public static void beforeAll() throws SQLException, ClassNotFoundException, IOException {
    testDir = TestHelper.createTmpDirPath("jdbc-parquet-test-");
    passwordPath = testDir.resolve(".password");
    sqlPath = testDir.resolve("query.sql");
    passwordPath.toFile().createNewFile();
    Files.write(sqlPath, "SELECT COF_NAME, SIZE, TOTAL FROM COFFEES WHERE SIZE >= 300".getBytes());
    DbTestHelper.createFixtures(CONNECTION_URL);
    try (Connection connection = DbTestHelper.createConnection(CONNECTION_URL)) {
      connection
          .createStatement()
          .execute("CREATE TABLE TS_MICROS (ID INT NOT NULL, CREATED_AT TIMESTAMP(6) NOT NULL)");
      connection
          .createStatement()
          .execute("INSERT INTO TS_MICROS VALUES (1, TIMESTAMP '2026-08-20 19:06:01.879123')");
    }
  }

  private static final class TimestampColumn {
    private final String logicalType;
    private final List<Long> values;

    private TimestampColumn(final String logicalType, final List<Long> values) {
      this.logicalType = logicalType;
      this.values = values;
    }
  }

  /** Reads CREATED_AT back the way Scio/Beam ParquetIO do: through the footer Avro schema. */
  private static TimestampColumn readCreatedAtWithAvro(final Path parquetFile) throws IOException {
    final List<Long> values = new ArrayList<>();
    String logicalType = null;
    try (ParquetReader<GenericRecord> reader =
        AvroParquetReader.<GenericRecord>builder(
                HadoopInputFile.fromPath(
                    new org.apache.hadoop.fs.Path(parquetFile.toUri()),
                    new org.apache.hadoop.conf.Configuration()))
            .build()) {
      GenericRecord record;
      while ((record = reader.read()) != null) {
        Schema fieldSchema = record.getSchema().getField("CREATED_AT").schema();
        if (fieldSchema.getType() == Schema.Type.UNION) {
          for (Schema branch : fieldSchema.getTypes()) {
            if (branch.getType() != Schema.Type.NULL) {
              fieldSchema = branch;
            }
          }
        }
        logicalType = fieldSchema.getProp("logicalType");
        values.add((Long) record.get("CREATED_AT"));
      }
    }
    return new TimestampColumn(logicalType, values);
  }

  private static Path runTimestampExport(final String name, final String... extraArgs)
      throws Exception {
    final Path outputPath = testDir.resolve(name);
    final List<String> args =
        new ArrayList<>(
            List.of(
                "--targetParallelism=1",
                "--partition=2025-02-28",
                "--skipPartitionCheck",
                "--exportTimeout=PT1M",
                "--connectionUrl=" + CONNECTION_URL,
                "--username=",
                "--passwordFile=" + passwordPath.toString(),
                "--table=TS_MICROS",
                "--output=" + outputPath,
                "--avroCodec=snappy",
                "--useLogicalTypes"));
    args.addAll(List.of(extraArgs));
    JdbcParquetJob.create(args.toArray(new String[0])).runExport();
    return outputPath.resolve("part-00000-of-00001.parquet");
  }

  @Test
  public void shouldRoundTripTimestampMicrosThroughFooterAvroSchema() throws Exception {
    final Path parquetFile =
        runTimestampExport("shouldRoundTripTimestampMicros", "--useTimestampMicros");

    final TimestampColumn column = readCreatedAtWithAvro(parquetFile);
    final long expectedMicros =
        ChronoUnit.MICROS.between(
            LocalDateTime.of(1970, 1, 1, 0, 0).atOffset(ZoneOffset.UTC),
            TS_MICROS_VALUE.atOffset(ZoneOffset.UTC));
    Assertions.assertEquals("timestamp-micros", column.logicalType);
    Assertions.assertEquals(List.of(expectedMicros), column.values);
    Assertions.assertEquals(123L, expectedMicros % 1000);
  }

  @Test
  public void shouldRoundTripTimestampMillisThroughFooterAvroSchema() throws Exception {
    final Path parquetFile = runTimestampExport("shouldRoundTripTimestampMillis");

    final TimestampColumn column = readCreatedAtWithAvro(parquetFile);
    final long expectedMillis = TS_MICROS_VALUE.toInstant(ZoneOffset.UTC).toEpochMilli();
    Assertions.assertEquals("timestamp-millis", column.logicalType);
    Assertions.assertEquals(List.of(expectedMillis), column.values);
  }

  @Test
  public void shouldFailWhenSchemaFileTimestampUnitDisagreesWithFlag() throws Exception {
    final Path schemaFile = testDir.resolve("ts_micros_schema.parquet.txt");
    Files.write(
        schemaFile,
        ("message TS_MICROS {\n"
                + "  optional int32 ID;\n"
                + "  optional int64 CREATED_AT (TIMESTAMP(MICROS,true));\n"
                + "}")
            .getBytes());

    final FailedValidationException exception =
        Assertions.assertThrows(
            FailedValidationException.class,
            () ->
                runTimestampExport(
                    "shouldFailWhenSchemaFileTimestampUnitDisagreesWithFlag",
                    "--parquetSchemaFilePath=" + schemaFile));
    Assertions.assertTrue(
        exception.getMessage().contains("CREATED_AT"), exception.getMessage());
  }

  @Test
  public void shouldRoundTripTimestampMicrosWithSchemaFile() throws Exception {
    final Path schemaFile = testDir.resolve("ts_micros_schema_ok.parquet.txt");
    Files.write(
        schemaFile,
        ("message renamed {\n"
                + "  optional int32 ID;\n"
                + "  optional int64 CREATED_AT (TIMESTAMP(MICROS,true));\n"
                + "}")
            .getBytes());

    final Path parquetFile =
        runTimestampExport(
            "shouldRoundTripTimestampMicrosWithSchemaFile",
            "--useTimestampMicros",
            "--parquetSchemaFilePath=" + schemaFile);

    final TimestampColumn column = readCreatedAtWithAvro(parquetFile);
    final long expectedMicros =
        ChronoUnit.MICROS.between(
            LocalDateTime.of(1970, 1, 1, 0, 0).atOffset(ZoneOffset.UTC),
            TS_MICROS_VALUE.atOffset(ZoneOffset.UTC));
    Assertions.assertEquals("timestamp-micros", column.logicalType);
    Assertions.assertEquals(List.of(expectedMicros), column.values);
  }

  private static Path unannotatedTimestampSchemaFile() throws IOException {
    final Path schemaFile = testDir.resolve("ts_unannotated_schema.parquet.txt");
    Files.write(
        schemaFile,
        ("message TS_MICROS {\n"
                + "  optional int32 ID;\n"
                + "  optional int64 CREATED_AT;\n"
                + "}")
            .getBytes());
    return schemaFile;
  }

  @Test
  public void shouldWriteMicrosForUnannotatedSchemaFileFieldWithFlag() throws Exception {
    final Path parquetFile =
        runTimestampExport(
            "shouldWriteMicrosForUnannotatedSchemaFileFieldWithFlag",
            "--useTimestampMicros",
            "--parquetSchemaFilePath=" + unannotatedTimestampSchemaFile());

    final TimestampColumn column = readCreatedAtWithAvro(parquetFile);
    final long expectedMicros =
        ChronoUnit.MICROS.between(
            LocalDateTime.of(1970, 1, 1, 0, 0).atOffset(ZoneOffset.UTC),
            TS_MICROS_VALUE.atOffset(ZoneOffset.UTC));
    Assertions.assertNull(column.logicalType);
    Assertions.assertEquals(List.of(expectedMicros), column.values);
  }

  @Test
  public void shouldWriteMillisForUnannotatedSchemaFileFieldWithoutFlag() throws Exception {
    final Path parquetFile =
        runTimestampExport(
            "shouldWriteMillisForUnannotatedSchemaFileFieldWithoutFlag",
            "--parquetSchemaFilePath=" + unannotatedTimestampSchemaFile());

    final TimestampColumn column = readCreatedAtWithAvro(parquetFile);
    Assertions.assertNull(column.logicalType);
    Assertions.assertEquals(
        List.of(TS_MICROS_VALUE.toInstant(ZoneOffset.UTC).toEpochMilli()), column.values);
  }

  @Test
  public void shouldFailWhenSchemaFileHasMillisWithFlag() throws Exception {
    final Path schemaFile = testDir.resolve("ts_millis_schema.parquet.txt");
    Files.write(
        schemaFile,
        ("message TS_MICROS {\n"
                + "  optional int32 ID;\n"
                + "  optional int64 CREATED_AT (TIMESTAMP(MILLIS,true));\n"
                + "}")
            .getBytes());
    final FailedValidationException exception =
        Assertions.assertThrows(
            FailedValidationException.class,
            () ->
                runTimestampExport(
                    "shouldFailWhenSchemaFileHasMillisWithFlag",
                    "--useTimestampMicros",
                    "--parquetSchemaFilePath=" + schemaFile));
    Assertions.assertTrue(
        exception.getMessage().contains("--useTimestampMicros is set"), exception.getMessage());
  }

  @Test
  public void shouldRunJdbcParquetJob() throws Exception {
    final Path outputPath = testDir.resolve("shouldRunJdbcParquetJob");

    JdbcParquetJob.create(
            new String[] {
              "--targetParallelism=1",
              "--partition=2025-02-28",
              "--skipPartitionCheck",
              "--exportTimeout=PT1M",
              "--connectionUrl=" + CONNECTION_URL,
              "--username=",
              "--passwordFile=" + passwordPath.toString(),
              "--table=COFFEES",
              "--output=" + outputPath,
              "--avroCodec=snappy"
            })
        .runExport();

    assertThat(
        TestHelper.listDir(outputPath.toFile()),
        containsInAnyOrder(
            "_AVRO_SCHEMA.avsc",
            "_PARQUET_SCHEMA.json",
            "_METRICS.json",
            "_SERVICE_METRICS.json",
            "_queries",
            "part-00000-of-00001.parquet"));
    assertThat(
        TestHelper.listDir(outputPath.resolve("_queries").toFile()),
        containsInAnyOrder("query_0.sql"));

    // Verify parquet schema file was written
    final String schemaJson =
        new String(Files.readAllBytes(outputPath.resolve("_PARQUET_SCHEMA.json")));
    Assertions.assertTrue(schemaJson.contains("COFFEES"));

    // Verify the parquet file has data by checking file size
    final File parquetFile = outputPath.resolve("part-00000-of-00001.parquet").toFile();
    assertThat(parquetFile.length(), greaterThan(0L));
  }

  @Test
  public void shouldRunJdbcParquetJobDataOnly() throws Exception {
    final Path outputPath = testDir.resolve("shouldRunJdbcParquetJobDataOnly");

    JdbcParquetJob.create(
            new String[] {
              "--targetParallelism=1",
              "--partition=2025-02-28",
              "--skipPartitionCheck",
              "--dataOnly=true",
              "--exportTimeout=PT1M",
              "--connectionUrl=" + CONNECTION_URL,
              "--username=",
              "--passwordFile=" + passwordPath.toString(),
              "--table=COFFEES",
              "--output=" + outputPath.toString(),
              "--avroCodec=snappy"
            })
        .runExport();

    assertThat(
        TestHelper.listDir(outputPath.toFile()), containsInAnyOrder("part-00000-of-00001.parquet"));
  }

  @Test
  public void shouldRunJdbcParquetJobSqlFile() throws Exception {
    final Path outputPath = testDir.resolve("shouldRunJdbcParquetJobSqlFile");

    JdbcParquetJob.create(
            new String[] {
              "--targetParallelism=1",
              "--partition=2025-02-28",
              "--skipPartitionCheck",
              "--exportTimeout=PT1M",
              "--connectionUrl=" + CONNECTION_URL,
              "--username=",
              "--passwordFile=" + passwordPath.toString(),
              "--output=" + outputPath,
              "--avroCodec=snappy",
              "--sqlFile=" + sqlPath.toString()
            })
        .runExport();

    assertThat(
        TestHelper.listDir(outputPath.toFile()),
        containsInAnyOrder(
            "_AVRO_SCHEMA.avsc",
            "_PARQUET_SCHEMA.json",
            "_METRICS.json",
            "_SERVICE_METRICS.json",
            "_queries",
            "part-00000-of-00001.parquet"));

    // Verify schema only has the 3 selected columns
    final String schemaJson =
        new String(Files.readAllBytes(outputPath.resolve("_PARQUET_SCHEMA.json")));
    Assertions.assertTrue(schemaJson.contains("COF_NAME"));
    Assertions.assertTrue(schemaJson.contains("SIZE"));
    Assertions.assertTrue(schemaJson.contains("TOTAL"));
    // Should only have a parquet file with data
    final File parquetFile = outputPath.resolve("part-00000-of-00001.parquet").toFile();
    assertThat(parquetFile.length(), greaterThan(0L));
  }

  @Test
  public void shouldRunParquetJobWithLogicalTypes() throws Exception {
    final Path outputPath = testDir.resolve("shouldRunParquetJobWithLogicalTypes");

    JdbcParquetJob.create(
            new String[] {
              "--targetParallelism=1",
              "--partition=2025-02-28",
              "--skipPartitionCheck",
              "--exportTimeout=PT1M",
              "--connectionUrl=" + CONNECTION_URL,
              "--username=",
              "--passwordFile=" + passwordPath.toString(),
              "--table=COFFEES",
              "--output=" + outputPath,
              "--avroCodec=snappy",
              "--useAvroLogicalTypes"
            })
        .runExport();

    assertThat(
        TestHelper.listDir(outputPath.toFile()),
        containsInAnyOrder(
            "_AVRO_SCHEMA.avsc",
            "_PARQUET_SCHEMA.json",
            "_METRICS.json",
            "_SERVICE_METRICS.json",
            "_queries",
            "part-00000-of-00001.parquet"));

    final String schemaJson =
        new String(Files.readAllBytes(outputPath.resolve("_PARQUET_SCHEMA.json")));
    Assertions.assertTrue(schemaJson.contains("TIMESTAMP"));
  }

  @Test
  public void shouldRunParquetJobWithTimestampMicros() throws Exception {
    final Path outputPath = testDir.resolve("shouldRunParquetJobWithTimestampMicros");

    JdbcParquetJob.create(
            new String[] {
              "--targetParallelism=1",
              "--partition=2025-02-28",
              "--skipPartitionCheck",
              "--exportTimeout=PT1M",
              "--connectionUrl=" + CONNECTION_URL,
              "--username=",
              "--passwordFile=" + passwordPath.toString(),
              "--table=COFFEES",
              "--output=" + outputPath,
              "--avroCodec=snappy",
              "--useLogicalTypes",
              "--useTimestampMicros"
            })
        .runExport();

    final String schemaJson =
        new String(Files.readAllBytes(outputPath.resolve("_PARQUET_SCHEMA.json")));
    Assertions.assertTrue(schemaJson.contains("TIMESTAMP(MICROS,true)"), schemaJson);
    Assertions.assertFalse(schemaJson.contains("MILLIS"), schemaJson);

    final String avroSchema =
        new String(Files.readAllBytes(outputPath.resolve("_AVRO_SCHEMA.avsc")));
    Assertions.assertTrue(avroSchema.contains("timestamp-micros"), avroSchema);
    Assertions.assertFalse(avroSchema.contains("timestamp-millis"), avroSchema);

    final org.apache.hadoop.fs.Path hadoopPath =
        new org.apache.hadoop.fs.Path(
            outputPath.resolve("part-00000-of-00001.parquet").toUri());
    try (org.apache.parquet.hadoop.ParquetFileReader fileReader =
        org.apache.parquet.hadoop.ParquetFileReader.open(
            org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
                hadoopPath, new org.apache.hadoop.conf.Configuration()))) {
      final String footerAvroSchema =
          fileReader
              .getFooter()
              .getFileMetaData()
              .getKeyValueMetaData()
              .get("parquet.avro.schema");
      Assertions.assertTrue(footerAvroSchema.contains("timestamp-micros"), footerAvroSchema);
    }
  }

  @Test
  public void shouldRunParquetJobWithMinRows() throws Exception {
    final Path outputPath = testDir.resolve("shouldRunParquetJobWithMinRows");

    JdbcParquetJob.create(
            new String[] {
              "--targetParallelism=1",
              "--partition=2025-02-28",
              "--skipPartitionCheck",
              "--exportTimeout=PT1M",
              "--connectionUrl=" + CONNECTION_URL,
              "--username=",
              "--passwordFile=" + passwordPath.toString(),
              "--table=COFFEES",
              "--output=" + outputPath,
              "--avroCodec=snappy",
              "--minRows=2"
            })
        .runExport();

    final File parquetFile = outputPath.resolve("part-00000-of-00001.parquet").toFile();
    assertThat(parquetFile.length(), greaterThan(0L));
  }

  @Test
  public void shouldRunParquetJobWithInputSchemaFile() throws Exception {
    final Path outputPath = testDir.resolve("shouldRunParquetJobWithInputSchemaFile");
    final Path schemaFile = testDir.resolve("input_schema.parquet.txt");
    Files.write(
        schemaFile,
        ("message COFFEES {\n"
                + "  optional binary COF_NAME (STRING);\n"
                + "  optional double SIZE;\n"
                + "  optional int64 TOTAL;\n"
                + "}")
            .getBytes());

    JdbcParquetJob.create(
            new String[] {
              "--targetParallelism=1",
              "--partition=2025-02-28",
              "--skipPartitionCheck",
              "--exportTimeout=PT1M",
              "--connectionUrl=" + CONNECTION_URL,
              "--username=",
              "--passwordFile=" + passwordPath.toString(),
              "--output=" + outputPath,
              "--avroCodec=snappy",
              "--parquetSchemaFilePath=" + schemaFile.toString(),
              "--sqlFile=" + sqlPath.toString()
            })
        .runExport();

    final String schemaJson =
        new String(Files.readAllBytes(outputPath.resolve("_PARQUET_SCHEMA.json")));
    Assertions.assertTrue(schemaJson.contains("COF_NAME"));
    Assertions.assertTrue(schemaJson.contains("SIZE"));
    Assertions.assertTrue(schemaJson.contains("TOTAL"));
    final File parquetFile = outputPath.resolve("part-00000-of-00001.parquet").toFile();
    assertThat(parquetFile.length(), greaterThan(0L));
  }

  @Test
  public void shouldFailParquetJobWithIncompatibleInputSchemaFile() throws Exception {
    final Path outputPath = testDir.resolve("shouldFailParquetJobWithIncompatibleInputSchemaFile");
    final Path schemaFile = testDir.resolve("incompatible_schema.parquet.txt");
    Files.write(
        schemaFile,
        ("message COFFEES {\n"
                + "  optional binary name (STRING);\n"
                + "  optional float SIZE;\n"
                + "  optional int64 TOTAL;\n"
                + "}")
            .getBytes());

    final JdbcParquetJob job =
        JdbcParquetJob.create(
            new String[] {
              "--targetParallelism=1",
              "--partition=2025-02-28",
              "--skipPartitionCheck",
              "--exportTimeout=PT1M",
              "--connectionUrl=" + CONNECTION_URL,
              "--username=",
              "--passwordFile=" + passwordPath.toString(),
              "--output=" + outputPath,
              "--parquetSchemaFilePath=" + schemaFile.toString(),
              "--sqlFile=" + sqlPath.toString()
            });
    final FailedValidationException e =
        Assertions.assertThrows(FailedValidationException.class, job::prepareExport);
    Assertions.assertTrue(e.getMessage().contains("SIZE (#2, JDBC column SIZE)"), e.getMessage());
  }

  @Test
  public void shouldRejectAvroSchemaFilePath() throws Exception {
    final Path outputPath = testDir.resolve("shouldRejectAvroSchemaFilePath");
    final IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                JdbcParquetJob.create(
                    new String[] {
                      "--targetParallelism=1",
                      "--partition=2025-02-28",
                      "--skipPartitionCheck",
                      "--connectionUrl=" + CONNECTION_URL,
                      "--username=",
                      "--passwordFile=" + passwordPath.toString(),
                      "--table=COFFEES",
                      "--output=" + outputPath,
                      "--avroSchemaFilePath=/does/not/exist.avsc"
                    }));
    Assertions.assertTrue(e.getMessage().contains("--parquetSchemaFilePath"), e.getMessage());
    Assertions.assertFalse(outputPath.toFile().exists());
  }

  private static CompressionCodecName readCodec(final Path parquetFile) throws IOException {
    try (ParquetFileReader reader =
        ParquetFileReader.open(
            HadoopInputFile.fromPath(
                new org.apache.hadoop.fs.Path(parquetFile.toUri()),
                new org.apache.hadoop.conf.Configuration()))) {
      return reader.getFooter().getBlocks().get(0).getColumns().get(0).getCodec();
    }
  }

  private static Path runCoffeesExport(final String name, final String... extraArgs)
      throws Exception {
    final Path outputPath = testDir.resolve(name);
    final List<String> args =
        new ArrayList<>(
            List.of(
                "--targetParallelism=1",
                "--partition=2025-02-28",
                "--skipPartitionCheck",
                "--exportTimeout=PT1M",
                "--connectionUrl=" + CONNECTION_URL,
                "--username=",
                "--passwordFile=" + passwordPath.toString(),
                "--table=COFFEES",
                "--output=" + outputPath));
    args.addAll(List.of(extraArgs));
    JdbcParquetJob.create(args.toArray(new String[0])).runExport();
    return outputPath.resolve("part-00000-of-00001.parquet");
  }

  @Test
  public void shouldDefaultToSnappyCodec() throws Exception {
    Assertions.assertEquals(
        CompressionCodecName.SNAPPY, readCodec(runCoffeesExport("shouldDefaultToSnappyCodec")));
  }

  @Test
  public void shouldIgnoreAvroCodec() throws Exception {
    Assertions.assertEquals(
        CompressionCodecName.SNAPPY,
        readCodec(runCoffeesExport("shouldIgnoreAvroCodecDeflate", "--avroCodec=deflate1")));
    Assertions.assertEquals(
        CompressionCodecName.SNAPPY,
        readCodec(runCoffeesExport("shouldIgnoreAvroCodecZstd", "--avroCodec=zstandard1")));
  }

  @Test
  public void shouldWarnOnlyForNonDefaultAvroCodec() {
    Assertions.assertTrue(
        JdbcParquetJob.warnIgnoredAvroCodec(
            JdbcParquetJob.buildPipelineOptions(new String[] {"--avroCodec=zstandard1"})));
    Assertions.assertFalse(
        JdbcParquetJob.warnIgnoredAvroCodec(JdbcParquetJob.buildPipelineOptions(new String[] {})));
  }

  @Test
  public void shouldUseParquetCodecOption() throws Exception {
    Assertions.assertEquals(
        CompressionCodecName.GZIP,
        readCodec(runCoffeesExport("shouldUseParquetCodecGzip", "--parquetCodec=gzip")));
    Assertions.assertEquals(
        CompressionCodecName.ZSTD,
        readCodec(
            runCoffeesExport(
                "shouldUseParquetCodecZstdLevel", "--avroCodec=deflate1", "--parquetCodec=zstd9")));
  }

  @Test
  public void shouldRejectInvalidParquetCodecBeforeAnyOutput() throws Exception {
    final Path outputPath = testDir.resolve("shouldRejectInvalidParquetCodec");
    final JdbcParquetJob job =
        JdbcParquetJob.create(
            new String[] {
              "--targetParallelism=1",
              "--partition=2025-02-28",
              "--skipPartitionCheck",
              "--connectionUrl=" + CONNECTION_URL,
              "--username=",
              "--passwordFile=" + passwordPath.toString(),
              "--table=COFFEES",
              "--output=" + outputPath,
              "--parquetCodec=deflate6"
            });
    final IllegalArgumentException e =
        Assertions.assertThrows(IllegalArgumentException.class, job::prepareExport);
    Assertions.assertTrue(
        e.getMessage().contains("Supported values: snappy, gzip"), e.getMessage());
    Assertions.assertFalse(outputPath.toFile().exists());
  }
}
