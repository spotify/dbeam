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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.sql.SQLException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class BenchJdbcParquetJobTest {

  private static final String CONNECTION_URL =
      "jdbc:h2:mem:testbench;MODE=PostgreSQL;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1";
  private static Path testDir;

  @BeforeClass
  public static void beforeAll() throws SQLException, ClassNotFoundException, IOException {
    testDir = TestHelper.createTmpDirPath("jdbc-parquet-bench-test");
    DbTestHelper.createFixtures(CONNECTION_URL);
  }

  @Test
  public void shouldRunBenchJdbcParquetJob() {
    BenchJdbcParquetJob.main(
        new String[] {
          "--targetParallelism=1",
          "--skipPartitionCheck",
          "--connectionUrl=" + CONNECTION_URL,
          "--username=",
          "--table=COFFEES",
          "--output=" + testDir.toString(),
          "--avroCodec=snappy",
          "--executions=2"
        });
    assertThat(TestHelper.listDir(testDir.toFile()), containsInAnyOrder("run_0", "run_1"));
  }

  @Test
  public void shouldOutputMetricsTsvSummary() throws IOException {
    final Path benchDir = TestHelper.createTmpDirPath("jdbc-parquet-bench-metrics");
    final ByteArrayOutputStream capturedOut = new ByteArrayOutputStream();
    final PrintStream originalOut = System.out;
    System.setOut(new PrintStream(capturedOut));
    try {
      BenchJdbcParquetJob.main(
          new String[] {
            "--targetParallelism=1",
            "--skipPartitionCheck",
            "--connectionUrl=" + CONNECTION_URL,
            "--username=",
            "--table=COFFEES",
            "--output=" + benchDir.toString(),
            "--avroCodec=snappy",
            "--executions=2"
          });
    } finally {
      System.setOut(originalOut);
    }

    final String output = capturedOut.toString();
    Assert.assertTrue(output.contains("Summary for BenchJdbcParquetJob"));
    Assert.assertTrue(output.contains("recordCount"));
    Assert.assertTrue(output.contains("writeElapsedMs"));
    Assert.assertTrue(output.contains("bytesWritten"));
    Assert.assertTrue(output.contains("KbWritePerSec"));
    Assert.assertTrue(output.contains("run_00"));
    Assert.assertTrue(output.contains("run_01"));
    Assert.assertTrue(output.contains("max"));
    Assert.assertTrue(output.contains("mean"));
    Assert.assertTrue(output.contains("min"));
    Assert.assertTrue(output.contains("stddev"));
    // Each run should report 2 records
    assertThat(output.split("run_").length, greaterThan(2));
  }
}
