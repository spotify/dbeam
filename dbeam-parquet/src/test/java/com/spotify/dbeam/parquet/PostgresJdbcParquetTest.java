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

import static org.mockito.Mockito.when;

import com.spotify.dbeam.TestHelper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class PostgresJdbcParquetTest {

  private ResultSet buildMockResultSet(ResultSetMetaData meta) throws SQLException {
    final ResultSet resultSet = Mockito.mock(ResultSet.class);
    when(resultSet.getMetaData()).thenReturn(meta);
    return resultSet;
  }

  @Test
  public void shouldEncodeUuidAsString() throws SQLException, IOException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn("test_table");
    TestHelper.mockResultSetMeta(meta, 1, Types.OTHER, "uuid_field", "java.util.UUID", "uuid");

    final ResultSet resultSet = buildMockResultSet(meta);
    final UUID uuidExpected = UUID.fromString("123e4567-e89b-12d3-a456-426655440000");
    when(resultSet.getObject(1)).thenReturn(uuidExpected);
    when(resultSet.wasNull()).thenReturn(false);

    final MessageType schema =
        JdbcParquetSchema.createParquetSchema(
            resultSet, Optional.empty(), false, "typed_first_row");

    final Path tempFile = Files.createTempFile("parquet-uuid-test-", ".parquet");
    Files.delete(tempFile);
    writeAndVerify(
        schema,
        resultSet,
        tempFile,
        record -> {
          String actualUuid = record.getString("uuid_field", 0);
          Assertions.assertEquals(uuidExpected.toString(), actualUuid);
        });
  }

  @Test
  public void shouldEncodeStringAndOtherTypes() throws SQLException, IOException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(2);
    when(meta.getTableName(1)).thenReturn("test_table");
    when(meta.getTableName(2)).thenReturn("test_table");
    TestHelper.mockResultSetMeta(meta, 1, Types.VARCHAR, "text_field", "java.lang.String", "text");
    TestHelper.mockResultSetMeta(
        meta, 2, Types.OTHER, "other_field", "java.util.UUID", "something_else");

    final ResultSet resultSet = buildMockResultSet(meta);
    when(resultSet.getString(1)).thenReturn("some_text_42");
    when(resultSet.getString(2)).thenReturn("some_other_42");
    when(resultSet.wasNull()).thenReturn(false);

    final MessageType schema =
        JdbcParquetSchema.createParquetSchema(
            resultSet, Optional.empty(), false, "typed_first_row");

    final Path tempFile = Files.createTempFile("parquet-string-test-", ".parquet");
    Files.delete(tempFile);
    writeAndVerify(
        schema,
        resultSet,
        tempFile,
        record -> {
          Assertions.assertEquals("some_text_42", record.getString("text_field", 0));
          Assertions.assertEquals("some_other_42", record.getString("other_field", 0));
        });
  }

  @Test
  public void shouldEncodeTimestampAsMillis() throws SQLException, IOException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn("test_table");
    TestHelper.mockResultSetMeta(
        meta, 1, Types.TIMESTAMP, "ts_field", "java.sql.Timestamp", "timestamp");

    final ResultSet resultSet = buildMockResultSet(meta);
    final Timestamp ts = new Timestamp(1488300933000L);
    when(resultSet.getTimestamp(Mockito.eq(1), Mockito.any())).thenReturn(ts);
    when(resultSet.wasNull()).thenReturn(false);

    final MessageType schema =
        JdbcParquetSchema.createParquetSchema(
            resultSet, Optional.empty(), false, "typed_first_row");

    final Path tempFile = Files.createTempFile("parquet-ts-test-", ".parquet");
    Files.delete(tempFile);
    writeAndVerify(
        schema,
        resultSet,
        tempFile,
        record -> {
          long actualTs = record.getLong("ts_field", 0);
          Assertions.assertEquals(1488300933000L, actualTs);
        });
  }

  @Test
  public void shouldHandleNullFields() throws SQLException, IOException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(2);
    when(meta.getTableName(1)).thenReturn("test_table");
    when(meta.getTableName(2)).thenReturn("test_table");
    TestHelper.mockResultSetMeta(meta, 1, Types.VARCHAR, "name", "java.lang.String", "varchar");
    TestHelper.mockResultSetMeta(meta, 2, Types.INTEGER, "age", "java.lang.Integer", "int4");

    final ResultSet resultSet = buildMockResultSet(meta);
    when(resultSet.getString(1)).thenReturn("alice");
    when(resultSet.getInt(2)).thenReturn(0);
    // First call for name (not null), second for age (null)
    when(resultSet.wasNull()).thenReturn(false, true);

    final MessageType schema =
        JdbcParquetSchema.createParquetSchema(
            resultSet, Optional.empty(), false, "typed_first_row");

    final Path tempFile = Files.createTempFile("parquet-null-test-", ".parquet");
    Files.delete(tempFile);
    writeAndVerify(
        schema,
        resultSet,
        tempFile,
        record -> {
          Assertions.assertEquals("alice", record.getString("name", 0));
          Assertions.assertEquals(0, record.getFieldRepetitionCount("age"));
        });
  }

  @Test
  public void shouldEncodeMultipleNumericTypes() throws SQLException, IOException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(4);
    when(meta.getTableName(1)).thenReturn("test_table");
    when(meta.getTableName(2)).thenReturn("test_table");
    when(meta.getTableName(3)).thenReturn("test_table");
    when(meta.getTableName(4)).thenReturn("test_table");
    TestHelper.mockResultSetMeta(meta, 1, Types.INTEGER, "int_col", "java.lang.Integer", "int4");
    TestHelper.mockResultSetMeta(meta, 2, Types.BIGINT, "long_col", "java.lang.Long", "int8");
    TestHelper.mockResultSetMeta(meta, 3, Types.FLOAT, "float_col", "java.lang.Float", "float4");
    TestHelper.mockResultSetMeta(meta, 4, Types.DOUBLE, "double_col", "java.lang.Double", "float8");

    final ResultSet resultSet = buildMockResultSet(meta);
    when(resultSet.getInt(1)).thenReturn(42);
    when(resultSet.getLong(2)).thenReturn(9999999999L);
    when(resultSet.getFloat(3)).thenReturn(3.14f);
    when(resultSet.getDouble(4)).thenReturn(2.71828);
    when(resultSet.wasNull()).thenReturn(false);

    final MessageType schema =
        JdbcParquetSchema.createParquetSchema(
            resultSet, Optional.empty(), false, "typed_first_row");

    final Path tempFile = Files.createTempFile("parquet-numeric-test-", ".parquet");
    Files.delete(tempFile);
    writeAndVerify(
        schema,
        resultSet,
        tempFile,
        record -> {
          Assertions.assertEquals(42, record.getInteger("int_col", 0));
          Assertions.assertEquals(9999999999L, record.getLong("long_col", 0));
          Assertions.assertEquals(3.14f, record.getFloat("float_col", 0), 0.001f);
          Assertions.assertEquals(2.71828, record.getDouble("double_col", 0), 0.00001);
        });
  }

  @Test
  public void shouldEncodeBooleanType() throws SQLException, IOException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn("test_table");
    TestHelper.mockResultSetMeta(meta, 1, Types.BOOLEAN, "is_active", "java.lang.Boolean", "bool");

    final ResultSet resultSet = buildMockResultSet(meta);
    when(resultSet.getBoolean(1)).thenReturn(true);
    when(resultSet.wasNull()).thenReturn(false);

    final MessageType schema =
        JdbcParquetSchema.createParquetSchema(
            resultSet, Optional.empty(), false, "typed_first_row");

    final Path tempFile = Files.createTempFile("parquet-bool-test-", ".parquet");
    Files.delete(tempFile);
    writeAndVerify(
        schema,
        resultSet,
        tempFile,
        record -> {
          Assertions.assertTrue(record.getBoolean("is_active", 0));
        });
  }

  @Test
  public void shouldEncodeArrayAsParquetList() throws SQLException, IOException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn("test_table");
    TestHelper.mockResultSetMeta(meta, 1, Types.ARRAY, "tags", "java.sql.Array", "_text");

    final ResultSet resultSet = buildMockResultSet(meta);
    final java.sql.Array mockArray = Mockito.mock(java.sql.Array.class);
    when(mockArray.getArray()).thenReturn(new String[] {"rock", "jazz", "blues"});
    when(resultSet.getArray(1)).thenReturn(mockArray);
    when(resultSet.wasNull()).thenReturn(false);

    final MessageType schema =
        JdbcParquetSchema.createParquetSchema(
            resultSet, Optional.empty(), false, "typed_first_row");

    // Verify schema has LIST type
    Assertions.assertFalse(schema.getFields().get(0).isPrimitive());
    Assertions.assertEquals(
        org.apache.parquet.schema.LogicalTypeAnnotation.listType(),
        schema.getFields().get(0).getLogicalTypeAnnotation());

    final Path tempFile = Files.createTempFile("parquet-array-test-", ".parquet");
    Files.delete(tempFile);
    writeAndVerify(
        schema,
        resultSet,
        tempFile,
        record -> {
          Group tagsList = record.getGroup("tags", 0);
          Assertions.assertEquals(3, tagsList.getFieldRepetitionCount("list"));
          Assertions.assertEquals("rock", tagsList.getGroup("list", 0).getString("element", 0));
          Assertions.assertEquals("jazz", tagsList.getGroup("list", 1).getString("element", 0));
          Assertions.assertEquals("blues", tagsList.getGroup("list", 2).getString("element", 0));
        });
  }

  @Test
  public void shouldEncodeIntegerArrayAsTypedList() throws SQLException, IOException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn("test_table");
    TestHelper.mockResultSetMeta(meta, 1, Types.ARRAY, "scores", "java.sql.Array", "_int4");

    final ResultSet resultSet = buildMockResultSet(meta);
    final java.sql.Array mockArray = Mockito.mock(java.sql.Array.class);
    when(mockArray.getArray()).thenReturn(new Integer[] {10, 20, 30});
    when(resultSet.getArray(1)).thenReturn(mockArray);
    when(resultSet.wasNull()).thenReturn(false);

    final MessageType schema =
        JdbcParquetSchema.createParquetSchema(
            resultSet, Optional.empty(), false, "typed_first_row");

    final Path tempFile = Files.createTempFile("parquet-int-array-test-", ".parquet");
    Files.delete(tempFile);
    writeAndVerify(
        schema,
        resultSet,
        tempFile,
        record -> {
          Group scoresList = record.getGroup("scores", 0);
          Assertions.assertEquals(3, scoresList.getFieldRepetitionCount("list"));
          Assertions.assertEquals(10, scoresList.getGroup("list", 0).getInteger("element", 0));
          Assertions.assertEquals(20, scoresList.getGroup("list", 1).getInteger("element", 0));
          Assertions.assertEquals(30, scoresList.getGroup("list", 2).getInteger("element", 0));
        });
  }

  @Test
  public void shouldEncodeBigintArrayAsTypedList() throws SQLException, IOException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn("test_table");
    TestHelper.mockResultSetMeta(meta, 1, Types.ARRAY, "ids", "java.sql.Array", "_int8");

    final ResultSet resultSet = buildMockResultSet(meta);
    final java.sql.Array mockArray = Mockito.mock(java.sql.Array.class);
    when(mockArray.getArray()).thenReturn(new Long[] {100L, 200L, 300L});
    when(resultSet.getArray(1)).thenReturn(mockArray);
    when(resultSet.wasNull()).thenReturn(false);

    final MessageType schema =
        JdbcParquetSchema.createParquetSchema(
            resultSet, Optional.empty(), false, "typed_first_row");

    final Path tempFile = Files.createTempFile("parquet-long-array-test-", ".parquet");
    Files.delete(tempFile);
    writeAndVerify(
        schema,
        resultSet,
        tempFile,
        record -> {
          Group idsList = record.getGroup("ids", 0);
          Assertions.assertEquals(3, idsList.getFieldRepetitionCount("list"));
          Assertions.assertEquals(100L, idsList.getGroup("list", 0).getLong("element", 0));
          Assertions.assertEquals(200L, idsList.getGroup("list", 1).getLong("element", 0));
          Assertions.assertEquals(300L, idsList.getGroup("list", 2).getLong("element", 0));
        });
  }

  @Test
  public void shouldHandleNullArrayElements() throws SQLException, IOException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn("test_table");
    TestHelper.mockResultSetMeta(meta, 1, Types.ARRAY, "tags", "java.sql.Array", "_text");

    final ResultSet resultSet = buildMockResultSet(meta);
    final java.sql.Array mockArray = Mockito.mock(java.sql.Array.class);
    when(mockArray.getArray()).thenReturn(new String[] {"first", null, "third"});
    when(resultSet.getArray(1)).thenReturn(mockArray);
    when(resultSet.wasNull()).thenReturn(false);

    final MessageType schema =
        JdbcParquetSchema.createParquetSchema(
            resultSet, Optional.empty(), false, "typed_first_row");

    final Path tempFile = Files.createTempFile("parquet-null-array-test-", ".parquet");
    Files.delete(tempFile);
    writeAndVerify(
        schema,
        resultSet,
        tempFile,
        record -> {
          Group tagsList = record.getGroup("tags", 0);
          Assertions.assertEquals(3, tagsList.getFieldRepetitionCount("list"));
          // First element: present
          Assertions.assertEquals("first", tagsList.getGroup("list", 0).getString("element", 0));
          // Second element: null (element field has 0 repetitions)
          Assertions.assertEquals(
              0, tagsList.getGroup("list", 1).getFieldRepetitionCount("element"));
          // Third element: present
          Assertions.assertEquals("third", tagsList.getGroup("list", 2).getString("element", 0));
        });
  }

  @Test
  public void shouldHandleNullSqlArray() throws SQLException, IOException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn("test_table");
    TestHelper.mockResultSetMeta(meta, 1, Types.ARRAY, "tags", "java.sql.Array", "_text");

    final ResultSet resultSet = buildMockResultSet(meta);
    when(resultSet.getArray(1)).thenReturn(null);
    when(resultSet.wasNull()).thenReturn(true);

    final MessageType schema =
        JdbcParquetSchema.createParquetSchema(
            resultSet, Optional.empty(), false, "typed_first_row");

    final Path tempFile = Files.createTempFile("parquet-null-sql-array-test-", ".parquet");
    Files.delete(tempFile);
    writeAndVerify(
        schema,
        resultSet,
        tempFile,
        record -> {
          // Entire array field is null (optional, 0 repetitions)
          Assertions.assertEquals(0, record.getFieldRepetitionCount("tags"));
        });
  }

  @FunctionalInterface
  interface RecordAssertion {
    void assertRecord(Group record) throws IOException;
  }

  private void writeAndVerify(
      MessageType schema, ResultSet resultSet, Path tempFile, RecordAssertion assertion)
      throws IOException {
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
      writer.write(resultSet);
    }

    final Configuration conf = new Configuration();
    final org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(tempFile.toUri());
    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), hadoopPath).withConf(conf).build()) {
      Group record = reader.read();
      Assertions.assertNotNull(record);
      assertion.assertRecord(record);
    }

    Files.deleteIfExists(tempFile);
  }
}
