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

import com.google.common.collect.Lists;

import com.spotify.dbeam.DbTestHelper;
import com.spotify.dbeam.TestHelper;
import com.spotify.dbeam.avro.JdbcAvroMetering;
import com.spotify.dbeam.options.OutputOptions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class JdbcAvroJobTest {

  private static String CONNECTION_URL =
      "jdbc:h2:mem:test2;MODE=PostgreSQL;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1";
  private static File DIR = TestHelper.createTmpDirName("jdbc-avro-test-").toFile();
  private static File PASSWORD_FILE = new File(DIR.getAbsolutePath() + ".password");

  private List<String> listDir(File dir) {
    return Arrays.stream(Objects.requireNonNull(dir.listFiles()))
        .map(File::getName).sorted().collect(Collectors.toList());
  }

  private List<GenericRecord> readAvroRecords(File avroFile, Schema schema) throws IOException {
    GenericDatumReader<GenericRecord> datum = new GenericDatumReader<>(schema);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(avroFile, datum);
    List<GenericRecord> records = StreamSupport.stream(dataFileReader.spliterator(), false)
        .collect(Collectors.toList());
    dataFileReader.close();
    return records;
  }

  @BeforeClass
  public static void beforeAll() throws SQLException, ClassNotFoundException, IOException {
    DbTestHelper.createFixtures(CONNECTION_URL);
    PASSWORD_FILE.createNewFile();
  }

  @AfterClass
  public static void afterAll() throws IOException {
    PASSWORD_FILE.delete();
    Files.walk(DIR.toPath())
        .sorted(Comparator.reverseOrder())
        .forEach(p -> p.toFile().delete());
  }

  @Test
  public void shouldRunJdbcAvroJob() throws IOException {
    JdbcAvroJob.main(new String[]{
        "--targetParallelism=1",  // no need for more threads when testing
        "--partition=2025-02-28",
        "--skipPartitionCheck",
        "--exportTimeout=PT1M",
        "--connectionUrl=" + CONNECTION_URL,
        "--username=",
        "--passwordFile=" + PASSWORD_FILE.getAbsolutePath(),
        "--table=COFFEES",
        "--output=" + DIR.getAbsolutePath(),
        "--avroCodec=zstandard1"
    });
    Assert.assertThat(
        listDir(DIR),
        Matchers.is(
            Lists.newArrayList("_AVRO_SCHEMA.avsc", "_METRICS.json",
                               "_SERVICE_METRICS.json", "_queries", "part-00000-of-00001.avro")
        ));
    Assert.assertThat(
        listDir(new File(DIR, "_queries")),
        Matchers.is(
            Lists.newArrayList("query_0.sql")
        ));
    Schema schema = new Schema.Parser().parse(new File(DIR, "_AVRO_SCHEMA.avsc"));
    List<GenericRecord> records =
        readAvroRecords(new File(DIR, "part-00000-of-00001.avro"), schema);
    Assert.assertEquals(2, records.size());
  }

  @Test
  public void shouldHaveDefaultExitCode() throws IOException, ClassNotFoundException {
    Assert.assertEquals(
        Integer.valueOf(49),
        ExceptionHandling.exitCode(new IllegalStateException())
    );
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
    metering.exposeWriteElapsedMs(0);
    metering.incrementRecordCount();
    metering.exposeWriteElapsedMs(0);
  }

}
