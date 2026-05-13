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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class PsqlParquetJobTest {

  private static Path testDir;
  private static Path passwordPath;

  @BeforeAll
  public static void beforeAll() throws IOException {
    testDir = TestHelper.createTmpDirPath("psql-parquet-test-");
    passwordPath = testDir.resolve(".password");
    passwordPath.toFile().createNewFile();
  }

  @Test
  public void shouldFailOnNonPostgresConnection() {
    final Path outputPath = testDir.resolve("shouldFailOnNonPostgres");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            PsqlParquetJob.create(
                new String[] {
                  "--connectionUrl=jdbc:h2:mem:test",
                  "--username=",
                  "--passwordFile=" + passwordPath.toString(),
                  "--table=COFFEES",
                  "--output=" + outputPath,
                  "--partition=2025-02-28"
                }));
  }

  @Test
  public void shouldFailOnMissingPartition() {
    final Path outputPath = testDir.resolve("shouldFailOnMissingPartition");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            PsqlParquetJob.create(
                new String[] {
                  "--connectionUrl=jdbc:postgresql://localhost/test",
                  "--username=",
                  "--passwordFile=" + passwordPath.toString(),
                  "--table=COFFEES",
                  "--output=" + outputPath,
                  "--skipPartitionCheck"
                }));
  }
}
