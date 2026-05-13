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
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class JdbcParquetRecordTest {

  private static final String CONNECTION_URL =
      "jdbc:h2:mem:testrecord;MODE=PostgreSQL;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1";

  @BeforeAll
  public static void beforeAll() throws SQLException, ClassNotFoundException {
    DbTestHelper.createFixtures(CONNECTION_URL);
  }

  @Test
  public void shouldCreateSchemaFromDatabase() throws ClassNotFoundException, SQLException {
    final Connection connection = DbTestHelper.createConnection(CONNECTION_URL);
    final MessageType schema =
        JdbcParquetSchema.createSchemaByReadingOneRow(
            connection,
            QueryBuilderArgs.create("COFFEES"),
            Optional.empty(),
            false,
            "typed_first_row");

    Assertions.assertNotNull(schema);
    Assertions.assertEquals("COFFEES", schema.getName());
    Assertions.assertEquals(14, schema.getFieldCount());
    Assertions.assertEquals("COF_NAME", schema.getFields().get(0).getName());
    Assertions.assertEquals("SUP_ID", schema.getFields().get(1).getName());
    Assertions.assertEquals("PRICE", schema.getFields().get(2).getName());
    Assertions.assertEquals("TEMPERATURE", schema.getFields().get(3).getName());
    Assertions.assertEquals("SIZE", schema.getFields().get(4).getName());
    Assertions.assertEquals("IS_ARABIC", schema.getFields().get(5).getName());
    Assertions.assertEquals("SALES", schema.getFields().get(6).getName());
    Assertions.assertEquals("TOTAL", schema.getFields().get(7).getName());
    Assertions.assertEquals("CREATED", schema.getFields().get(8).getName());
    Assertions.assertEquals("UPDATED", schema.getFields().get(9).getName());
    Assertions.assertEquals("UID", schema.getFields().get(10).getName());
    Assertions.assertEquals("ROWNUM", schema.getFields().get(11).getName());
    Assertions.assertEquals("INT_ARR", schema.getFields().get(12).getName());
    Assertions.assertEquals("TEXT_ARR", schema.getFields().get(13).getName());
  }

  @Test
  public void shouldCreateSchemaWithLogicalTypes() throws ClassNotFoundException, SQLException {
    final Connection connection = DbTestHelper.createConnection(CONNECTION_URL);
    final MessageType schema =
        JdbcParquetSchema.createSchemaByReadingOneRow(
            connection,
            QueryBuilderArgs.create("COFFEES"),
            Optional.empty(),
            true,
            "typed_first_row");

    Assertions.assertEquals(14, schema.getFieldCount());
    // CREATED and UPDATED should have timestamp logical type
    Assertions.assertNotNull(
        schema.getFields().get(8).asPrimitiveType().getLogicalTypeAnnotation());
    Assertions.assertNotNull(
        schema.getFields().get(9).asPrimitiveType().getLogicalTypeAnnotation());
  }

  @Test
  public void shouldCreateSchemaWithCustomName() throws ClassNotFoundException, SQLException {
    final Connection connection = DbTestHelper.createConnection(CONNECTION_URL);
    final MessageType schema =
        JdbcParquetSchema.createSchemaByReadingOneRow(
            connection,
            QueryBuilderArgs.create("COFFEES"),
            Optional.of("CustomSchema"),
            false,
            "typed_first_row");

    Assertions.assertEquals("CustomSchema", schema.getName());
  }

  @Test
  public void shouldWriteAndReadBackParquetRecords()
      throws ClassNotFoundException, SQLException, IOException {
    final Connection connection = DbTestHelper.createConnection(CONNECTION_URL);
    final MessageType schema =
        JdbcParquetSchema.createSchemaByReadingOneRow(
            connection,
            QueryBuilderArgs.create("COFFEES"),
            Optional.empty(),
            false,
            "typed_first_row");

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
    final org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(tempFile.toUri());
    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), hadoopPath).withConf(conf).build()) {
      Group record1 = reader.read();
      Assertions.assertNotNull(record1);
      // Verify first record has expected fields
      String cofName = record1.getString("COF_NAME", 0);
      Assertions.assertNotNull(cofName);

      Group record2 = reader.read();
      Assertions.assertNotNull(record2);

      // Should only be 2 records
      Group record3 = reader.read();
      Assertions.assertNull(record3);
    }

    // Cleanup
    Files.deleteIfExists(tempFile);
  }

  @Test
  public void shouldWriteCorrectFieldValues()
      throws ClassNotFoundException, SQLException, IOException {
    final Connection connection = DbTestHelper.createConnection(CONNECTION_URL);
    final MessageType schema =
        JdbcParquetSchema.createSchemaByReadingOneRow(
            connection,
            QueryBuilderArgs.create("COFFEES"),
            Optional.empty(),
            false,
            "typed_first_row");

    final Path tempFile = Files.createTempFile("parquet-values-test-", ".parquet");
    Files.delete(tempFile);

    final ResultSet rs =
        connection.createStatement().executeQuery("SELECT * FROM COFFEES ORDER BY COF_NAME");

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
    final org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(tempFile.toUri());
    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), hadoopPath).withConf(conf).build()) {
      Group record = reader.read();
      Assertions.assertNotNull(record);

      // Verify typed values for the first record (colombian caffee, sorted)
      String cofName = record.getString("COF_NAME", 0);
      Assertions.assertEquals("colombian caffee", cofName);

      boolean isArabic = record.getBoolean("IS_ARABIC", 0);
      Assertions.assertTrue(isArabic);

      int sales = record.getInteger("SALES", 0);
      Assertions.assertEquals(13, sales);

      long total = record.getLong("TOTAL", 0);
      Assertions.assertEquals(201L, total);

      float temperature = record.getFloat("TEMPERATURE", 0);
      Assertions.assertEquals(87.5f, temperature, 0.01f);

      double size = record.getDouble("SIZE", 0);
      Assertions.assertEquals(230.7, size, 0.01);

      long rownum = record.getLong("ROWNUM", 0);
      Assertions.assertEquals(2L, rownum);
    }

    Files.deleteIfExists(tempFile);
  }

  @Test
  public void shouldHandleNullValues() throws ClassNotFoundException, SQLException, IOException {
    // SUP_ID and UPDATED are null in the Coffee fixtures
    final Connection connection = DbTestHelper.createConnection(CONNECTION_URL);
    final MessageType schema =
        JdbcParquetSchema.createSchemaByReadingOneRow(
            connection,
            QueryBuilderArgs.create("COFFEES"),
            Optional.empty(),
            false,
            "typed_first_row");

    final Path tempFile = Files.createTempFile("parquet-null-test-", ".parquet");
    Files.delete(tempFile);

    final ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM COFFEES LIMIT 1");

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
    final org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(tempFile.toUri());
    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), hadoopPath).withConf(conf).build()) {
      Group record = reader.read();
      Assertions.assertNotNull(record);

      // SUP_ID is null - should have 0 repetitions in Parquet
      Assertions.assertEquals(0, record.getFieldRepetitionCount("SUP_ID"));

      // UPDATED is null
      Assertions.assertEquals(0, record.getFieldRepetitionCount("UPDATED"));

      // Non-null fields should have 1 repetition
      Assertions.assertEquals(1, record.getFieldRepetitionCount("COF_NAME"));
      Assertions.assertEquals(1, record.getFieldRepetitionCount("IS_ARABIC"));
    }

    Files.deleteIfExists(tempFile);
  }

  @Test
  public void shouldWriteAvroSchemaInParquetFooter()
      throws ClassNotFoundException, SQLException, IOException {
    final Connection connection = DbTestHelper.createConnection(CONNECTION_URL);
    final MessageType schema =
        JdbcParquetSchema.createSchemaByReadingOneRow(
            connection,
            QueryBuilderArgs.create("COFFEES"),
            Optional.empty(),
            false,
            "typed_first_row");

    final String avroSchemaJson =
        "{\"type\":\"record\",\"name\":\"COFFEES\","
            + "\"namespace\":\"dbeam_generated\",\"fields\":[]}";

    final Path tempFile = Files.createTempFile("parquet-footer-test-", ".parquet");
    Files.delete(tempFile);

    final ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM COFFEES LIMIT 1");

    final OutputFile outputFile =
        new ChannelOutputFile(
            java.nio.channels.FileChannel.open(
                tempFile,
                java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.WRITE));

    try (ParquetWriter<ResultSet> writer =
        new JdbcParquetIO.ResultSetParquetWriterBuilder(outputFile, schema, avroSchemaJson)
            .withWriteMode(ParquetFileWriter.Mode.CREATE)
            .build()) {
      while (rs.next()) {
        writer.write(rs);
      }
    }

    // Read back footer metadata and verify parquet.avro.schema key
    final Configuration conf = new Configuration();
    final org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(tempFile.toUri());
    try (ParquetFileReader fileReader =
        ParquetFileReader.open(HadoopInputFile.fromPath(hadoopPath, conf))) {
      final Map<String, String> keyValueMetaData =
          fileReader.getFooter().getFileMetaData().getKeyValueMetaData();
      Assertions.assertTrue(keyValueMetaData.containsKey(JdbcParquetIO.PARQUET_AVRO_SCHEMA_KEY));
      final String actualAvroSchema = keyValueMetaData.get(JdbcParquetIO.PARQUET_AVRO_SCHEMA_KEY);
      Assertions.assertEquals(avroSchemaJson, actualAvroSchema);
    }

    Files.deleteIfExists(tempFile);
  }

  @Test
  public void shouldOmitAvroSchemaWhenNotProvided()
      throws ClassNotFoundException, SQLException, IOException {
    final Connection connection = DbTestHelper.createConnection(CONNECTION_URL);
    final MessageType schema =
        JdbcParquetSchema.createSchemaByReadingOneRow(
            connection,
            QueryBuilderArgs.create("COFFEES"),
            Optional.empty(),
            false,
            "typed_first_row");

    final Path tempFile = Files.createTempFile("parquet-no-avro-test-", ".parquet");
    Files.delete(tempFile);

    final ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM COFFEES LIMIT 1");

    final OutputFile outputFile =
        new ChannelOutputFile(
            java.nio.channels.FileChannel.open(
                tempFile,
                java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.WRITE));

    // No avroSchemaJson provided (2-arg constructor)
    try (ParquetWriter<ResultSet> writer =
        new JdbcParquetIO.ResultSetParquetWriterBuilder(outputFile, schema)
            .withWriteMode(ParquetFileWriter.Mode.CREATE)
            .build()) {
      while (rs.next()) {
        writer.write(rs);
      }
    }

    final Configuration conf = new Configuration();
    final org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(tempFile.toUri());
    try (ParquetFileReader fileReader =
        ParquetFileReader.open(HadoopInputFile.fromPath(hadoopPath, conf))) {
      final Map<String, String> keyValueMetaData =
          fileReader.getFooter().getFileMetaData().getKeyValueMetaData();
      Assertions.assertFalse(keyValueMetaData.containsKey(JdbcParquetIO.PARQUET_AVRO_SCHEMA_KEY));
    }

    Files.deleteIfExists(tempFile);
  }
}
