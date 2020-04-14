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
import com.spotify.dbeam.avro.JdbcAvroMetering;
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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class JdbcAvroJobTest {

  private static final String CONNECTION_URL =
      "jdbc:h2:mem:test5;MODE=PostgreSQL;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1";
  private static Path testDir;
  private static Path passwordPath;
  private static Path sqlPath;

  private List<GenericRecord> readAvroRecords(File avroFile, Schema schema) throws IOException {
    GenericDatumReader<GenericRecord> datum = new GenericDatumReader<>(schema);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(avroFile, datum);
    List<GenericRecord> records =
        StreamSupport.stream(dataFileReader.spliterator(), false).collect(Collectors.toList());
    dataFileReader.close();
    return records;
  }

  @BeforeClass
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
    Path outputPath = testDir.resolve("shouldRunJdbcAvroJob");

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
          "--output=" + outputPath.toString(),
          "--avroCodec=zstandard1"
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
    Schema schema = new Schema.Parser().parse(outputPath.resolve("_AVRO_SCHEMA.avsc").toFile());
    List<GenericRecord> records =
        readAvroRecords(outputPath.resolve("part-00000-of-00001.avro").toFile(), schema);
    assertThat(records, hasSize(2));
  }

  @Test
  public void shouldRunJdbcAvroJobSqlFile() throws IOException {
    Path outputPath = testDir.resolve("shouldRunJdbcAvroJobSqlFile");

    JdbcAvroJob.main(
        new String[] {
          "--targetParallelism=1", // no need for more threads when testing
          "--partition=2025-02-28",
          "--skipPartitionCheck",
          "--exportTimeout=PT1M",
          "--connectionUrl=" + CONNECTION_URL,
          "--username=",
          "--passwordFile=" + passwordPath.toString(),
          "--output=" + outputPath.toString(),
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
    Schema schema = new Schema.Parser().parse(outputPath.resolve("_AVRO_SCHEMA.avsc").toFile());
    List<GenericRecord> records =
        readAvroRecords(outputPath.resolve("part-00000-of-00001.avro").toFile(), schema);
    assertThat(records, hasSize(1));
    assertThat(
        schema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList()),
        contains("COF_NAME", "SIZE", "TOTAL"));
  }

  @Test
  public void shouldRunAvroJobPreCommands() throws SQLException, ClassNotFoundException {
    Path outputPath = testDir.resolve("shouldRunAvroJobPreCommands");

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
          "--output=" + outputPath.toString(),
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

    List<String> schemas = new ArrayList<>();
    try (Connection connection = DbTestHelper.createConnection(CONNECTION_URL)) {
      ResultSet rs = connection.createStatement().executeQuery("SHOW SCHEMAS;");
      while (rs.next()) {
        schemas.add(rs.getString(1));
      }
    }

    String[] expectedSchemas = {"TEST_COMMAND_1", "TEST_COMMAND_2"};
    assertThat(schemas, CoreMatchers.hasItems(expectedSchemas));
  }

  @Test
  public void shouldHaveDefaultExitCode() throws IOException, ClassNotFoundException {
    Assert.assertEquals(
        Integer.valueOf(49), ExceptionHandling.exitCode(new IllegalStateException()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailOnMissingInput() throws IOException, ClassNotFoundException {
    JdbcAvroJob.create(PipelineOptionsFactory.create());
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailOnEmptyInput() throws IOException, ClassNotFoundException {
    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
    pipelineOptions.as(OutputOptions.class).setOutput("");
    JdbcAvroJob.create(PipelineOptionsFactory.create());
  }

  @Test
  public void shouldIncrementCounterMetrics() {
    JdbcAvroMetering metering = new JdbcAvroMetering(1, 1);
    metering.startWriteMeter();
    metering.exposeWriteElapsed();
    metering.incrementRecordCount();
    metering.exposeWriteElapsed();
  }
}
