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

import com.spotify.dbeam.TestHelper;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.BeforeClass;
import org.junit.Test;

public class PsqlParquetJobTest {

  private static Path testDir;
  private static Path passwordPath;

  @BeforeClass
  public static void beforeAll() throws IOException {
    testDir = TestHelper.createTmpDirPath("psql-parquet-test-");
    passwordPath = testDir.resolve(".password");
    passwordPath.toFile().createNewFile();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailOnNonPostgresConnection() throws Exception {
    final Path outputPath = testDir.resolve("shouldFailOnNonPostgres");
    PsqlParquetJob.create(
        new String[] {
          "--connectionUrl=jdbc:h2:mem:test",
          "--username=",
          "--passwordFile=" + passwordPath.toString(),
          "--table=COFFEES",
          "--output=" + outputPath,
          "--partition=2025-02-28"
        });
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailOnMissingPartition() throws Exception {
    final Path outputPath = testDir.resolve("shouldFailOnMissingPartition");
    PsqlParquetJob.create(
        new String[] {
          "--connectionUrl=jdbc:postgresql://localhost/test",
          "--username=",
          "--passwordFile=" + passwordPath.toString(),
          "--table=COFFEES",
          "--output=" + outputPath,
          "--skipPartitionCheck"
        });
  }
}
