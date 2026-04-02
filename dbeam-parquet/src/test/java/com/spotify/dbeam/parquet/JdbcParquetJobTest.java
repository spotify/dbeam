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
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class JdbcParquetJobTest {

  private static final String CONNECTION_URL =
      "jdbc:h2:mem:testparquet;MODE=PostgreSQL;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1";
  private static Path testDir;
  private static Path passwordPath;
  private static Path sqlPath;

  @BeforeClass
  public static void beforeAll() throws SQLException, ClassNotFoundException, IOException {
    testDir = TestHelper.createTmpDirPath("jdbc-parquet-test-");
    passwordPath = testDir.resolve(".password");
    sqlPath = testDir.resolve("query.sql");
    passwordPath.toFile().createNewFile();
    Files.write(sqlPath, "SELECT COF_NAME, SIZE, TOTAL FROM COFFEES WHERE SIZE >= 300".getBytes());
    DbTestHelper.createFixtures(CONNECTION_URL);
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
    Assert.assertTrue(schemaJson.contains("COFFEES"));

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
        TestHelper.listDir(outputPath.toFile()),
        containsInAnyOrder("part-00000-of-00001.parquet"));
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
            "_PARQUET_SCHEMA.json",
            "_METRICS.json",
            "_SERVICE_METRICS.json",
            "_queries",
            "part-00000-of-00001.parquet"));

    // Verify schema only has the 3 selected columns
    final String schemaJson =
        new String(Files.readAllBytes(outputPath.resolve("_PARQUET_SCHEMA.json")));
    Assert.assertTrue(schemaJson.contains("COF_NAME"));
    Assert.assertTrue(schemaJson.contains("SIZE"));
    Assert.assertTrue(schemaJson.contains("TOTAL"));
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
            "_PARQUET_SCHEMA.json",
            "_METRICS.json",
            "_SERVICE_METRICS.json",
            "_queries",
            "part-00000-of-00001.parquet"));

    final String schemaJson =
        new String(Files.readAllBytes(outputPath.resolve("_PARQUET_SCHEMA.json")));
    Assert.assertTrue(schemaJson.contains("TIMESTAMP"));
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
  public void shouldMapAvroCodecToParquetCodec() {
    Assert.assertEquals("snappy", JdbcParquetJob.mapAvroCodecToParquetCodec("snappy"));
    Assert.assertEquals("gzip", JdbcParquetJob.mapAvroCodecToParquetCodec("deflate1"));
    Assert.assertEquals("gzip", JdbcParquetJob.mapAvroCodecToParquetCodec("deflate6"));
    Assert.assertEquals("gzip", JdbcParquetJob.mapAvroCodecToParquetCodec("deflate9"));
    Assert.assertEquals("zstd", JdbcParquetJob.mapAvroCodecToParquetCodec("zstandard1"));
    Assert.assertEquals("zstd", JdbcParquetJob.mapAvroCodecToParquetCodec("zstandard9"));
  }

  @Test
  public void shouldRunParquetJobWithDeflateCodec() throws Exception {
    final Path outputPath = testDir.resolve("shouldRunParquetJobWithDeflateCodec");

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
              "--avroCodec=deflate1"
            })
        .runExport();

    final File parquetFile = outputPath.resolve("part-00000-of-00001.parquet").toFile();
    assertThat(parquetFile.length(), greaterThan(0L));
  }

  @Test
  public void shouldRunParquetJobWithZstandardCodec() throws Exception {
    final Path outputPath = testDir.resolve("shouldRunParquetJobWithZstandardCodec");

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
              "--avroCodec=zstandard1"
            })
        .runExport();

    final File parquetFile = outputPath.resolve("part-00000-of-00001.parquet").toFile();
    assertThat(parquetFile.length(), greaterThan(0L));
  }
}
