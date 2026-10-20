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

import com.spotify.dbeam.DbTestHelper;
import com.spotify.dbeam.TestHelper;
import com.spotify.dbeam.args.JdbcConnectionArgs;
import java.io.Closeable;
import java.io.File;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Optional;
import java.util.UUID;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class JdbcParquetIOTest {

  private static final String CONNECTION_URL =
      "jdbc:h2:mem:testparquetio;MODE=PostgreSQL;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1";
  private static Path testDir;

  @BeforeAll
  public static void beforeAll() throws Exception {
    testDir = TestHelper.createTmpDirPath("jdbc-parquet-io-test-");
    DbTestHelper.createFixtures(CONNECTION_URL);
  }

  private static FileBasedSink.Writer<Void, String> createWriter(final String name)
      throws Exception {
    final MessageType schema;
    try (Connection connection = DbTestHelper.createConnection(CONNECTION_URL);
        ResultSet rs =
            connection.createStatement().executeQuery("SELECT COF_NAME, TOTAL FROM COFFEES")) {
      rs.next();
      schema = JdbcParquetSchema.createParquetSchema(rs, Optional.empty(), false, "bytes");
    }
    final JdbcParquetArgs args =
        JdbcParquetArgs.create(
            JdbcConnectionArgs.create(CONNECTION_URL)
                .withUsername("")
                .withPassword(""));
    return JdbcParquetIO.createSink(
            testDir.resolve(name).toString(), ".parquet", schema, args, null)
        .createWriteOperation()
        .createWriter();
  }

  @Test
  public void shouldCountBytesWrittenOnceForWriterWithSeveralQueries() throws Exception {
    final MetricsContainerImpl container = new MetricsContainerImpl("step");
    final FileBasedSink.Writer<Void, String> writer = createWriter("severalQueries");
    try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(container)) {
      writer.open(UUID.randomUUID().toString());
      writer.write("SELECT COF_NAME, TOTAL FROM COFFEES");
      writer.write("SELECT COF_NAME, TOTAL FROM COFFEES");
      writer.write("SELECT COF_NAME, TOTAL FROM COFFEES");
    }
    // Beam closes writers in @FinishBundle, where the DirectRunner has no metrics container
    writer.close();

    final File file = new File(writer.getOutputFile().toString());
    final long bytesWritten =
        container
            .getCounter(MetricName.named(JdbcParquetMetering.class, "bytesWritten"))
            .getCumulative();
    Assertions.assertEquals(file.length(), bytesWritten);
    try (ParquetFileReader reader =
        ParquetFileReader.open(
            HadoopInputFile.fromPath(
                new org.apache.hadoop.fs.Path(file.toURI()),
                new org.apache.hadoop.conf.Configuration()))) {
      Assertions.assertEquals(6, reader.getRecordCount());
    }
  }
}
