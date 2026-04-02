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
import com.spotify.dbeam.args.QueryBuilderArgs;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class JdbcParquetRecordTest {

  private static final String CONNECTION_URL =
      "jdbc:h2:mem:testrecord;MODE=PostgreSQL;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1";

  @BeforeClass
  public static void beforeAll() throws SQLException, ClassNotFoundException {
    DbTestHelper.createFixtures(CONNECTION_URL);
  }

  @Test
  public void shouldCreateSchemaFromDatabase() throws ClassNotFoundException, SQLException {
    final Connection connection = DbTestHelper.createConnection(CONNECTION_URL);
    final MessageType schema = JdbcParquetSchema.createSchemaByReadingOneRow(
        connection, QueryBuilderArgs.create("COFFEES"), Optional.empty(), false);

    Assert.assertNotNull(schema);
    Assert.assertEquals("COFFEES", schema.getName());
    Assert.assertEquals(14, schema.getFieldCount());
    Assert.assertEquals("COF_NAME", schema.getFields().get(0).getName());
    Assert.assertEquals("SUP_ID", schema.getFields().get(1).getName());
    Assert.assertEquals("PRICE", schema.getFields().get(2).getName());
    Assert.assertEquals("TEMPERATURE", schema.getFields().get(3).getName());
    Assert.assertEquals("SIZE", schema.getFields().get(4).getName());
    Assert.assertEquals("IS_ARABIC", schema.getFields().get(5).getName());
    Assert.assertEquals("SALES", schema.getFields().get(6).getName());
    Assert.assertEquals("TOTAL", schema.getFields().get(7).getName());
    Assert.assertEquals("CREATED", schema.getFields().get(8).getName());
    Assert.assertEquals("UPDATED", schema.getFields().get(9).getName());
    Assert.assertEquals("UID", schema.getFields().get(10).getName());
    Assert.assertEquals("ROWNUM", schema.getFields().get(11).getName());
    Assert.assertEquals("INT_ARR", schema.getFields().get(12).getName());
    Assert.assertEquals("TEXT_ARR", schema.getFields().get(13).getName());
  }

  @Test
  public void shouldCreateSchemaWithLogicalTypes() throws ClassNotFoundException, SQLException {
    final Connection connection = DbTestHelper.createConnection(CONNECTION_URL);
    final MessageType schema = JdbcParquetSchema.createSchemaByReadingOneRow(
        connection, QueryBuilderArgs.create("COFFEES"), Optional.empty(), true);

    Assert.assertEquals(14, schema.getFieldCount());
    // CREATED and UPDATED should have timestamp logical type
    Assert.assertNotNull(schema.getFields().get(8).asPrimitiveType().getLogicalTypeAnnotation());
    Assert.assertNotNull(schema.getFields().get(9).asPrimitiveType().getLogicalTypeAnnotation());
  }

  @Test
  public void shouldCreateSchemaWithCustomName() throws ClassNotFoundException, SQLException {
    final Connection connection = DbTestHelper.createConnection(CONNECTION_URL);
    final MessageType schema = JdbcParquetSchema.createSchemaByReadingOneRow(
        connection, QueryBuilderArgs.create("COFFEES"), Optional.of("CustomSchema"), false);

    Assert.assertEquals("CustomSchema", schema.getName());
  }

  @Test
  public void shouldWriteAndReadBackParquetRecords()
      throws ClassNotFoundException, SQLException, IOException {
    final Connection connection = DbTestHelper.createConnection(CONNECTION_URL);
    final MessageType schema = JdbcParquetSchema.createSchemaByReadingOneRow(
        connection, QueryBuilderArgs.create("COFFEES"), Optional.empty(), false);

    // Write records to a temp file
    final Path tempFile = Files.createTempFile("parquet-test-", ".parquet");
    Files.delete(tempFile); // ParquetWriter needs to create the file itself

    final ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM COFFEES");

    final OutputFile outputFile =
        new ChannelOutputFile(
            java.nio.channels.FileChannel.open(
                tempFile,
                java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.WRITE));

    try (ParquetWriter<ResultSet> writer =
        new JdbcParquetIO.ResultSetParquetWriterBuilder(outputFile, schema)
            .withWriteMode(ParquetFileWriter.Mode.CREATE)
            .build()) {
      while (rs.next()) {
        writer.write(rs);
      }
    }

    // Read back and verify
    final Configuration conf = new Configuration();
    final org.apache.hadoop.fs.Path hadoopPath =
        new org.apache.hadoop.fs.Path(tempFile.toUri());
    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), hadoopPath)
            .withConf(conf)
            .build()) {
      Group record1 = reader.read();
      Assert.assertNotNull(record1);
      // Verify first record has expected fields
      String cofName = record1.getString("COF_NAME", 0);
      Assert.assertNotNull(cofName);

      Group record2 = reader.read();
      Assert.assertNotNull(record2);

      // Should only be 2 records
      Group record3 = reader.read();
      Assert.assertNull(record3);
    }

    // Cleanup
    Files.deleteIfExists(tempFile);
  }

  @Test
  public void shouldWriteCorrectFieldValues()
      throws ClassNotFoundException, SQLException, IOException {
    final Connection connection = DbTestHelper.createConnection(CONNECTION_URL);
    final MessageType schema = JdbcParquetSchema.createSchemaByReadingOneRow(
        connection, QueryBuilderArgs.create("COFFEES"), Optional.empty(), false);

    final Path tempFile = Files.createTempFile("parquet-values-test-", ".parquet");
    Files.delete(tempFile);

    final ResultSet rs = connection.createStatement().executeQuery(
        "SELECT * FROM COFFEES ORDER BY COF_NAME");

    final OutputFile outputFile =
        new ChannelOutputFile(
            java.nio.channels.FileChannel.open(
                tempFile,
                java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.WRITE));

    try (ParquetWriter<ResultSet> writer =
        new JdbcParquetIO.ResultSetParquetWriterBuilder(outputFile, schema)
            .withWriteMode(ParquetFileWriter.Mode.CREATE)
            .build()) {
      while (rs.next()) {
        writer.write(rs);
      }
    }

    final Configuration conf = new Configuration();
    final org.apache.hadoop.fs.Path hadoopPath =
        new org.apache.hadoop.fs.Path(tempFile.toUri());
    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), hadoopPath)
            .withConf(conf)
            .build()) {
      Group record = reader.read();
      Assert.assertNotNull(record);

      // Verify typed values for the first record (colombian caffee, sorted)
      String cofName = record.getString("COF_NAME", 0);
      Assert.assertEquals("colombian caffee", cofName);

      boolean isArabic = record.getBoolean("IS_ARABIC", 0);
      Assert.assertTrue(isArabic);

      int sales = record.getInteger("SALES", 0);
      Assert.assertEquals(13, sales);

      long total = record.getLong("TOTAL", 0);
      Assert.assertEquals(201L, total);

      float temperature = record.getFloat("TEMPERATURE", 0);
      Assert.assertEquals(87.5f, temperature, 0.01f);

      double size = record.getDouble("SIZE", 0);
      Assert.assertEquals(230.7, size, 0.01);

      long rownum = record.getLong("ROWNUM", 0);
      Assert.assertEquals(2L, rownum);
    }

    Files.deleteIfExists(tempFile);
  }

  @Test
  public void shouldHandleNullValues()
      throws ClassNotFoundException, SQLException, IOException {
    // SUP_ID and UPDATED are null in the Coffee fixtures
    final Connection connection = DbTestHelper.createConnection(CONNECTION_URL);
    final MessageType schema = JdbcParquetSchema.createSchemaByReadingOneRow(
        connection, QueryBuilderArgs.create("COFFEES"), Optional.empty(), false);

    final Path tempFile = Files.createTempFile("parquet-null-test-", ".parquet");
    Files.delete(tempFile);

    final ResultSet rs = connection.createStatement().executeQuery(
        "SELECT * FROM COFFEES LIMIT 1");

    final OutputFile outputFile =
        new ChannelOutputFile(
            java.nio.channels.FileChannel.open(
                tempFile,
                java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.WRITE));

    try (ParquetWriter<ResultSet> writer =
        new JdbcParquetIO.ResultSetParquetWriterBuilder(outputFile, schema)
            .withWriteMode(ParquetFileWriter.Mode.CREATE)
            .build()) {
      while (rs.next()) {
        writer.write(rs);
      }
    }

    final Configuration conf = new Configuration();
    final org.apache.hadoop.fs.Path hadoopPath =
        new org.apache.hadoop.fs.Path(tempFile.toUri());
    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), hadoopPath)
            .withConf(conf)
            .build()) {
      Group record = reader.read();
      Assert.assertNotNull(record);

      // SUP_ID is null - should have 0 repetitions in Parquet
      Assert.assertEquals(0, record.getFieldRepetitionCount("SUP_ID"));

      // UPDATED is null
      Assert.assertEquals(0, record.getFieldRepetitionCount("UPDATED"));

      // Non-null fields should have 1 repetition
      Assert.assertEquals(1, record.getFieldRepetitionCount("COF_NAME"));
      Assert.assertEquals(1, record.getFieldRepetitionCount("IS_ARABIC"));
    }

    Files.deleteIfExists(tempFile);
  }
}
