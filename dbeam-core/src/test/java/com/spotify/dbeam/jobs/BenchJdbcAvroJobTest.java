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
import static org.hamcrest.Matchers.containsInAnyOrder;

import com.spotify.dbeam.DbTestHelper;
import com.spotify.dbeam.TestHelper;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import org.junit.BeforeClass;
import org.junit.Test;

public class BenchJdbcAvroJobTest {

  private static final String CONNECTION_URL =
      "jdbc:h2:mem:test3;MODE=PostgreSQL;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1";
  private static Path testDir;

  @BeforeClass
  public static void beforeAll() throws SQLException, ClassNotFoundException, IOException {
    testDir = TestHelper.createTmpDirPath("jdbc-export-args-test");
    DbTestHelper.createFixtures(CONNECTION_URL);
  }

  @Test
  public void shouldRunJdbcAvroJob() {
    BenchJdbcAvroJob.main(
        new String[] {
          "--targetParallelism=1", // no need for more threads when testing
          "--skipPartitionCheck",
          "--connectionUrl=" + CONNECTION_URL,
          "--username=",
          "--table=COFFEES",
          "--output=" + testDir.toString(),
          "--avroCodec=zstandard1",
          "--executions=2"
        });
    assertThat(TestHelper.listDir(testDir.toFile()), containsInAnyOrder("run_0", "run_1"));
  }
}
