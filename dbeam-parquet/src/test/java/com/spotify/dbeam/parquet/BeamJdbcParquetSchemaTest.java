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
import com.spotify.dbeam.args.JdbcAvroArgs;
import com.spotify.dbeam.args.JdbcConnectionArgs;
import com.spotify.dbeam.args.JdbcExportArgs;
import com.spotify.dbeam.args.QueryBuilderArgs;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.parquet.schema.MessageType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class BeamJdbcParquetSchemaTest {

  private static final String CONNECTION_URL =
      "jdbc:h2:mem:testbeamschema;MODE=PostgreSQL;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1";

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void beforeAll() throws SQLException, ClassNotFoundException {
    DbTestHelper.createFixtures(CONNECTION_URL);
  }

  private static JdbcExportArgs createArgs() throws ClassNotFoundException {
    return JdbcExportArgs.create(
        JdbcAvroArgs.create(JdbcConnectionArgs.create(CONNECTION_URL)),
        QueryBuilderArgs.create("COFFEES"),
        "dbeam_generated",
        Optional.empty(),
        Optional.empty(),
        false,
        Duration.ofMinutes(1),
        Optional.empty());
  }

  @Test
  public void shouldCreateSchemaAndExposeMetrics() throws Exception {
    final JdbcExportArgs args = createArgs();
    try (Connection connection = args.createConnection()) {
      final MessageType schema =
          BeamJdbcParquetSchema.createSchema(pipeline, args, connection);

      Assert.assertNotNull(schema);
      Assert.assertEquals("COFFEES", schema.getName());
      Assert.assertEquals(14, schema.getFieldCount());
    }
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void shouldParseInputSchemaFile() throws IOException {
    final Path schemaFile = Files.createTempFile("test-schema-", ".parquet.txt");
    Files.write(schemaFile, (
        "message test_table {\n"
        + "  optional int64 id;\n"
        + "  optional binary name (STRING);\n"
        + "}").getBytes());

    final Optional<MessageType> schema =
        BeamJdbcParquetSchema.parseOptionalInputParquetSchemaFile(schemaFile.toString());

    Assert.assertTrue(schema.isPresent());
    Assert.assertEquals("test_table", schema.get().getName());
    Assert.assertEquals(2, schema.get().getFieldCount());

    Files.deleteIfExists(schemaFile);
  }

  @Test
  public void shouldReturnEmptyForNullFilename() throws IOException {
    final Optional<MessageType> schema =
        BeamJdbcParquetSchema.parseOptionalInputParquetSchemaFile(null);

    Assert.assertFalse(schema.isPresent());
  }

  @Test
  public void shouldReturnEmptyForEmptyFilename() throws IOException {
    final Optional<MessageType> schema =
        BeamJdbcParquetSchema.parseOptionalInputParquetSchemaFile("");

    Assert.assertFalse(schema.isPresent());
  }
}
