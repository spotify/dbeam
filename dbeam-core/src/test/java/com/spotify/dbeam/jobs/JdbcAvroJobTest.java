/*-
 * -\-\-
 * DBeam Core
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
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

package com.spotify.dbeam.jobs;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;

import com.spotify.dbeam.DbTestHelper;
import com.spotify.dbeam.TestHelper;
import com.spotify.dbeam.avro.JdbcMetering;
import com.spotify.dbeam.options.DBeamPipelineOptions;
import com.spotify.dbeam.options.OutputOptions;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class JdbcAvroJobTest {

  private static final String CONNECTION_URL =
      "jdbc:h2:mem:test5;MODE=PostgreSQL;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1";
  private static Path testDir;
  private static Path passwordPath;
  private static Path sqlPath;

  private List<GenericRecord> readAvroRecords(File avroFile, Schema schema) throws IOException {
    final GenericDatumReader<GenericRecord> datum = new GenericDatumReader<>(schema);
    final DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(avroFile, datum);
    final List<GenericRecord> records =
        StreamSupport.stream(dataFileReader.spliterator(), false).collect(Collectors.toList());
    dataFileReader.close();
    return records;
  }

  @BeforeAll
  public static void beforeAll() throws SQLException, ClassNotFoundException, IOException {
    testDir = TestHelper.createTmpDirPath("jdbc-avro-test-");
    passwordPath = testDir.resolve(".password");
    sqlPath = testDir.resolve("query.sql");
    passwordPath.toFile().createNewFile();
    Files.write(sqlPath, "SELECT COF_NAME, SIZE, TOTAL FROM COFFEES WHERE SIZE >= 300".getBytes());
    DbTestHelper.createFixtures(CONNECTION_URL);
  }

  @Test
  public void shouldRunJdbcAvroJob() throws IOException {
    final Path outputPath = testDir.resolve("shouldRunJdbcAvroJob");

    JdbcAvroJob.main(
        new String[] {
          "--targetParallelism=1", // no need for more threads when testing
          "--partition=2025-02-28",
          "--skipPartitionCheck",
          "--exportTimeout=PT1M",
          "--connectionUrl=" + CONNECTION_URL,
          "--username=",
          "--passwordFile=" + passwordPath.toString(),
          "--table=COFFEES",
          "--output=" + outputPath,
          "--avroCodec=zstandard1",
          "--minRows=2"
        });

    assertThat(
        TestHelper.listDir(outputPath.toFile()),
        containsInAnyOrder(
            "_AVRO_SCHEMA.avsc",
            "_METRICS.json",
            "_SERVICE_METRICS.json",
            "_queries",
            "part-00000-of-00001.avro"));
    assertThat(
        TestHelper.listDir(outputPath.resolve("_queries").toFile()),
        containsInAnyOrder("query_0.sql"));
    final Schema schema =
        new Schema.Parser().parse(outputPath.resolve("_AVRO_SCHEMA.avsc").toFile());
    List<GenericRecord> records =
        readAvroRecords(outputPath.resolve("part-00000-of-00001.avro").toFile(), schema);
    assertThat(records, hasSize(2));
  }

  @Test
  public void shouldRunJdbcAvroJobDataOnly() throws IOException {
    final Path outputPath = testDir.resolve("shouldRunJdbcAvroJobDataOnly");

    JdbcAvroJob.main(
        new String[] {
          "--targetParallelism=1", // no need for more threads when testing
          "--partition=2025-02-28",
          "--skipPartitionCheck",
          "--dataOnly=true",
          "--exportTimeout=PT1M",
          "--connectionUrl=" + CONNECTION_URL,
          "--username=",
          "--passwordFile=" + passwordPath.toString(),
          "--table=COFFEES",
          "--output=" + outputPath.toString(),
          "--avroCodec=zstandard1"
        });

    assertThat(
        TestHelper.listDir(outputPath.toFile()), containsInAnyOrder("part-00000-of-00001.avro"));
  }

  @Test
  public void shouldRunJdbcAvroJobSqlFile() throws IOException {
    final Path outputPath = testDir.resolve("shouldRunJdbcAvroJobSqlFile");

    JdbcAvroJob.main(
        new String[] {
          "--targetParallelism=1", // no need for more threads when testing
          "--partition=2025-02-28",
          "--skipPartitionCheck",
          "--exportTimeout=PT1M",
          "--connectionUrl=" + CONNECTION_URL,
          "--username=",
          "--passwordFile=" + passwordPath.toString(),
          "--output=" + outputPath,
          "--avroCodec=zstandard1",
          "--sqlFile=" + sqlPath.toString()
        });

    assertThat(
        TestHelper.listDir(outputPath.toFile()),
        containsInAnyOrder(
            "_AVRO_SCHEMA.avsc",
            "_METRICS.json",
            "_SERVICE_METRICS.json",
            "_queries",
            "part-00000-of-00001.avro"));
    assertThat(
        TestHelper.listDir(outputPath.resolve("_queries").toFile()),
        containsInAnyOrder("query_0.sql"));
    final Schema schema =
        new Schema.Parser().parse(outputPath.resolve("_AVRO_SCHEMA.avsc").toFile());
    List<GenericRecord> records =
        readAvroRecords(outputPath.resolve("part-00000-of-00001.avro").toFile(), schema);
    assertThat(records, hasSize(1));
    assertThat(
        schema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList()),
        contains("COF_NAME", "SIZE", "TOTAL"));
  }

  @Test
  public void shouldRunAvroJobPreCommands() throws SQLException, ClassNotFoundException {
    final Path outputPath = testDir.resolve("shouldRunAvroJobPreCommands");

    JdbcAvroJob.main(
        new String[] {
          "--targetParallelism=1", // no need for more threads when testing
          "--partition=2025-02-28",
          "--skipPartitionCheck",
          "--exportTimeout=PT1M",
          "--connectionUrl=" + CONNECTION_URL,
          "--username=",
          "--passwordFile=" + passwordPath.toString(),
          "--table=COFFEES",
          "--output=" + outputPath,
          "--avroCodec=zstandard1",
          "--preCommand=CREATE SCHEMA IF NOT EXISTS TEST_COMMAND_1;",
          "--preCommand=CREATE SCHEMA IF NOT EXISTS TEST_COMMAND_2;"
        });

    assertThat(
        TestHelper.listDir(outputPath.toFile()),
        containsInAnyOrder(
            "_AVRO_SCHEMA.avsc",
            "_METRICS.json",
            "_SERVICE_METRICS.json",
            "_queries",
            "part-00000-of-00001.avro"));

    final List<String> schemas = new ArrayList<>();
    try (Connection connection = DbTestHelper.createConnection(CONNECTION_URL)) {
      ResultSet rs = connection.createStatement().executeQuery("SHOW SCHEMAS;");
      while (rs.next()) {
        schemas.add(rs.getString(1));
      }
    }

    final String[] expectedSchemas = {"TEST_COMMAND_1", "TEST_COMMAND_2"};
    assertThat(schemas, CoreMatchers.hasItems(expectedSchemas));
  }

  private static Path createdSchemaFile(final String name, final String logicalType)
      throws IOException {
    final Path schemaFile = testDir.resolve(name + ".avsc");
    Files.write(
        schemaFile,
        ("{\"type\":\"record\",\"name\":\"created\",\"fields\":[{\"name\":\"CREATED\","
                + "\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\""
                + logicalType
                + "\"}]}]}")
            .getBytes());
    return schemaFile;
  }

  private static String[] createdOnlyArgs(final Path outputPath, final String... extraArgs)
      throws IOException {
    final Path createdSql = testDir.resolve("created.sql");
    Files.write(createdSql, "SELECT CREATED FROM COFFEES".getBytes());
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
                "--sqlFile=" + createdSql,
                "--output=" + outputPath,
                "--useLogicalTypes"));
    args.addAll(List.of(extraArgs));
    return args.toArray(new String[0]);
  }

  @Test
  public void shouldFailFastWhenSchemaFileTimestampUnitDisagreesWithFlag() throws IOException {
    final Path outputPath = testDir.resolve("shouldFailFastOnSchemaFileTimestampUnit");
    final Path schemaFile = createdSchemaFile("created_millis", "timestamp-millis");

    final FailedValidationException exception =
        Assertions.assertThrows(
            FailedValidationException.class,
            () ->
                JdbcAvroJob.create(
                        createdOnlyArgs(
                            outputPath,
                            "--useTimestampMicros",
                            "--avroSchemaFilePath=" + schemaFile))
                    .runExport());
    Assertions.assertTrue(exception.getMessage().contains("CREATED"), exception.getMessage());
    Assertions.assertFalse(outputPath.toFile().exists());
  }

  private static String[] coffeesArgs(final Path outputPath, final String... extraArgs) {
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
                "--output=" + outputPath,
                "--useLogicalTypes"));
    args.addAll(List.of(extraArgs));
    return args.toArray(new String[0]);
  }

  /** The schema dbeam generates for COFFEES, with every field renamed and docs/props changed. */
  private static String renamedCoffeesSchema() throws Exception {
    final Path generated = testDir.resolve("coffeesSchemaSource");
    JdbcAvroJob.create(coffeesArgs(generated)).runExport();
    final Schema schema =
        new Schema.Parser().parse(generated.resolve("_AVRO_SCHEMA.avsc").toFile());
    final StringBuilder sb =
        new StringBuilder(
            "{\"type\":\"record\",\"name\":\"renamed\",\"namespace\":\"other.ns\","
                + "\"doc\":\"custom\",\"fields\":[");
    for (int i = 0; i < schema.getFields().size(); i++) {
      final Schema.Field field = schema.getFields().get(i);
      sb.append(i == 0 ? "" : ",")
          .append("{\"name\":\"renamed_")
          .append(field.name().toLowerCase())
          .append("\",\"doc\":\"custom doc\",\"type\":")
          .append(field.schema().toString())
          .append(",\"default\":null}");
    }
    return sb.append("]}").toString();
  }

  @Test
  public void shouldAcceptRenamedSchemaFile() throws Exception {
    final Path outputPath = testDir.resolve("shouldAcceptRenamedSchemaFile");
    final Path schemaFile = testDir.resolve("coffees_renamed.avsc");
    Files.write(schemaFile, renamedCoffeesSchema().getBytes());

    JdbcAvroJob.create(coffeesArgs(outputPath, "--avroSchemaFilePath=" + schemaFile))
        .runExport();

    final Schema schema =
        new Schema.Parser().parse(outputPath.resolve("_AVRO_SCHEMA.avsc").toFile());
    final List<GenericRecord> records =
        readAvroRecords(outputPath.resolve("part-00000-of-00001.avro").toFile(), schema);
    assertThat(records, hasSize(2));
    assertThat(
        records.stream()
            .map(r -> r.get("renamed_cof_name").toString())
            .collect(Collectors.toList()),
        containsInAnyOrder("costa rica caffee", "colombian caffee"));
  }

  @Test
  public void shouldFailFastWhenSchemaFileTypeDoesNotMatchJdbcColumn() throws Exception {
    final Path outputPath = testDir.resolve("shouldFailFastWhenSchemaFileTypeDoesNotMatch");
    final Path schemaFile = testDir.resolve("coffees_decimal.avsc");
    // PRICE is a DECIMAL column, which dbeam writes as a string
    Files.write(
        schemaFile,
        renamedCoffeesSchema()
            .replaceFirst(
                "(\"renamed_price\",\"doc\":\"custom doc\",\"type\":\\[\"null\",)\"string\"",
                "$1{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":21,"
                    + "\"scale\":2}")
            .getBytes());

    final FailedValidationException exception =
        Assertions.assertThrows(
            FailedValidationException.class,
            () ->
                JdbcAvroJob.create(coffeesArgs(outputPath, "--avroSchemaFilePath=" + schemaFile))
                    .runExport());
    Assertions.assertTrue(
        exception.getMessage().contains("renamed_price (#3, JDBC column PRICE): expected string"),
        exception.getMessage());
    Assertions.assertFalse(outputPath.toFile().exists());
  }

  @Test
  public void shouldFailFastWhenSchemaFileArrayItemsDoNotMatch() throws Exception {
    final Path outputPath = testDir.resolve("shouldFailFastWhenSchemaFileArrayItemsDoNotMatch");
    final Path schemaFile = testDir.resolve("coffees_array.avsc");
    Files.write(
        schemaFile,
        renamedCoffeesSchema()
            .replace("{\"type\":\"array\",\"items\":\"int\"}",
                "{\"type\":\"array\",\"items\":\"string\"}")
            .getBytes());

    final FailedValidationException exception =
        Assertions.assertThrows(
            FailedValidationException.class,
            () ->
                JdbcAvroJob.create(coffeesArgs(outputPath, "--avroSchemaFilePath=" + schemaFile))
                    .runExport());
    Assertions.assertTrue(
        exception.getMessage().contains("JDBC column INT_ARR)[]: expected int"),
        exception.getMessage());
    Assertions.assertFalse(outputPath.toFile().exists());
  }

  @Test
  public void shouldWriteMicrosWithMatchingSchemaFile() throws Exception {
    final Path outputPath = testDir.resolve("shouldWriteMicrosWithMatchingSchemaFile");
    final Path schemaFile = createdSchemaFile("created_micros", "timestamp-micros");

    JdbcAvroJob.create(
            createdOnlyArgs(
                outputPath, "--useTimestampMicros", "--avroSchemaFilePath=" + schemaFile))
        .runExport();

    final Schema schema =
        new Schema.Parser().parse(outputPath.resolve("_AVRO_SCHEMA.avsc").toFile());
    final List<Long> values =
        readAvroRecords(outputPath.resolve("part-00000-of-00001.avro").toFile(), schema).stream()
            .map(r -> (Long) r.get("CREATED"))
            .sorted()
            .collect(Collectors.toList());
    Assertions.assertEquals(List.of(1488300723000000L, 1488300933000000L), values);
  }

  @Test
  public void shouldFailWithNotEnoughRows() {
    final Path outputPath = testDir.resolve("shouldRunJdbcAvroJob");

    Assertions.assertThrows(
        FailedValidationException.class,
        () ->
            JdbcAvroJob.create(
                    new String[] {
                      "--targetParallelism=1", // no need for more threads when testing
                      "--partition=2025-02-28",
                      "--skipPartitionCheck",
                      "--connectionUrl=" + CONNECTION_URL,
                      "--username=",
                      "--passwordFile=" + passwordPath.toString(),
                      "--table=COFFEES",
                      "--output=" + outputPath,
                      "--minRows=1000"
                    })
                .runExport());
  }

  @Test
  public void shouldConfigureDBeamVersionPipelineOptions() throws Exception {
    final Path outputPath = testDir.resolve("shouldRunJdbcAvroJob");

    final JdbcAvroJob jdbcAvroJob =
        JdbcAvroJob.create(
            new String[] {
              "--partition=2025-02-28",
              "--skipPartitionCheck",
              "--connectionUrl=" + CONNECTION_URL,
              "--username=",
              "--passwordFile=" + passwordPath.toString(),
              "--table=COFFEES",
              "--output=" + outputPath
            });
    jdbcAvroJob.prepareExport();

    Assertions.assertEquals(
        this.getClass().getPackage().getImplementationVersion(),
        jdbcAvroJob.getPipelineOptions().as(DBeamPipelineOptions.class).getDBeamVersion());
  }

  @Test
  public void shouldHaveDefaultExitCode() {
    Assertions.assertEquals(
        Integer.valueOf(49), ExceptionHandling.exitCode(new IllegalStateException()));
  }

  @Test
  public void shouldExit50OnFailedValidationException() {
    Assertions.assertEquals(
        Integer.valueOf(50), ExceptionHandling.exitCode(new FailedValidationException("")));
  }

  @Test
  public void shouldFailOnMissingInput() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> JdbcAvroJob.create(PipelineOptionsFactory.create()));
  }

  @Test
  public void shouldFailOnEmptyInput() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          final PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
          pipelineOptions.as(OutputOptions.class).setOutput("");
          JdbcAvroJob.create(PipelineOptionsFactory.create());
        });
  }

  @Test
  public void shouldIncrementCounterMetrics() {
    final JdbcMetering metering = new JdbcMetering(1, 1, "jdbcavroio");
    metering.startWriteMeter();
    metering.exposeWriteElapsed();
    metering.incrementRecordCount();
    metering.exposeWriteElapsed();
  }

  @Test
  public void shouldFailOnNormalizedFieldNameCollision() throws Exception {
    final Path sql = testDir.resolve("collide-avro.sql");
    Files.write(
        sql,
        "SELECT COF_NAME AS \"cof-name\", COF_NAME AS \"cof_name\", TOTAL FROM COFFEES"
            .getBytes());
    final Path outputPath = testDir.resolve("collide-avro");
    final JdbcAvroJob job =
        JdbcAvroJob.create(
            new String[] {
              "--skipPartitionCheck",
              "--connectionUrl=" + CONNECTION_URL,
              "--username=",
              "--passwordFile=" + passwordPath.toString(),
              "--sqlFile=" + sql,
              "--output=" + outputPath
            });
    final RuntimeException e = Assertions.assertThrows(RuntimeException.class, job::prepareExport);
    Assertions.assertTrue(
        e.getMessage().contains("columns [cof-name, cof_name] all map to field name 'cof_name'"),
        e.getMessage());
    Assertions.assertFalse(outputPath.toFile().exists());
  }
}
