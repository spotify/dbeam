/*-
 * -\-\-
 * DBeam Core
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

package com.spotify.dbeam.avro;

import com.spotify.dbeam.DbTestHelper;
import com.spotify.dbeam.TestHelper;
import com.spotify.dbeam.args.JdbcAvroArgs;
import com.spotify.dbeam.args.JdbcConnectionArgs;
import java.io.Closeable;
import java.io.File;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Optional;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class JdbcAvroIOTest {

  private static final String CONNECTION_URL =
      "jdbc:h2:mem:testavroio;MODE=PostgreSQL;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1";
  private static Path testDir;

  @BeforeAll
  public static void beforeAll() throws Exception {
    testDir = TestHelper.createTmpDirPath("jdbc-avro-io-test-");
    DbTestHelper.createFixtures(CONNECTION_URL);
  }

  @Test
  public void shouldCountBytesWrittenOnceForWriterWithSeveralQueries() throws Exception {
    final Schema schema;
    try (Connection connection = DbTestHelper.createConnection(CONNECTION_URL);
        ResultSet rs =
            connection.createStatement().executeQuery("SELECT COF_NAME, TOTAL FROM COFFEES")) {
      rs.next();
      schema =
          JdbcAvroSchema.createAvroSchema(
              rs, "ns", CONNECTION_URL, Optional.empty(), "doc", false, false, "bytes", false);
    }
    final JdbcAvroArgs args =
        JdbcAvroArgs.create(
            JdbcConnectionArgs.create(CONNECTION_URL).withUsername("").withPassword(""));
    final FileBasedSink.Writer<Void, String> writer =
        JdbcAvroIO.createSink(testDir.resolve("severalQueries").toString(), ".avro", schema, args)
            .createWriteOperation()
            .createWriter();

    final MetricsContainerImpl container = new MetricsContainerImpl("step");
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
            .getCounter(MetricName.named(JdbcAvroMetering.class, "bytesWritten"))
            .getCumulative();
    Assertions.assertEquals(file.length(), bytesWritten);
    long records = 0;
    try (DataFileReader<GenericRecord> reader =
        new DataFileReader<>(file, new GenericDatumReader<>(schema))) {
      while (reader.hasNext()) {
        reader.next();
        records++;
      }
    }
    Assertions.assertEquals(6, records);
  }
}
