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

import static org.mockito.Mockito.when;

import com.spotify.dbeam.TestHelper;
import com.spotify.dbeam.options.ArrayHandlingMode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class PostgresJdbcAvroTest {

  public static GenericRecord[] bytesToGenericRecords(Schema schema,
                                                          ByteBuffer... avroRecordBytes)
      throws IOException {
    DataFileWriter<GenericRecord> dataFileWriter =
        new DataFileWriter<>(new GenericDatumWriter<>(schema));
    ByteArrayOutputStream avroOutputStream = new ByteArrayOutputStream();
    dataFileWriter.create(schema, avroOutputStream);
    for (ByteBuffer b : avroRecordBytes) {
      dataFileWriter.appendEncoded(b);
    }

    dataFileWriter.close();

    SeekableByteArrayInput inputStream = new SeekableByteArrayInput(avroOutputStream.toByteArray());
    List<GenericRecord> genericRecords = new ArrayList<>();
    DataFileReader<GenericRecord> dataReader = new DataFileReader<>(inputStream,
        new GenericDatumReader<>(schema));
    while (dataReader.hasNext()) {
      genericRecords.add(dataReader.next());
    }
    return genericRecords.toArray(new GenericRecord[0]);
  }

  public void assertGenericRecordArrayField(GenericRecord record, String fieldName,
                                       String... expectedItems) {
    Utf8[] expectedItemsConverted = new Utf8[expectedItems.length];
    for (int i = 0; i < expectedItems.length; i++) {
      expectedItemsConverted[i] = expectedItems[i] != null ? new Utf8(expectedItems[i]) : null;
    }
    assertGenericRecordArrayField(record, fieldName, (Object[]) expectedItemsConverted);
  }

  public void assertGenericRecordArrayField(GenericRecord record, String fieldName,
                                       Object... expectedItems) {

    final GenericData.Array<GenericRecord> arrayValue =
        (GenericData.Array<GenericRecord>) record.get(fieldName);
    Assert.assertEquals(expectedItems.length, arrayValue.size());

    for (int i = 0; i < expectedItems.length; i++) {
      Assert.assertEquals(expectedItems[i], arrayValue.get(i));
    }
  }

  @Test
  public void shouldEncodeUUIDValues() throws SQLException, IOException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(2);
    TestHelper.mockResultSetMeta(meta, 1, Types.OTHER, "uuid_field", "java.util.UUID", "uuid");
    TestHelper.mockResultSetMeta(meta, 2, Types.ARRAY, "array_field", "java.sql.Array", "_uuid");

    final ResultSet resultSet = Mockito.mock(ResultSet.class);
    when(resultSet.getMetaData()).thenReturn(meta);
    final UUID uuidExpected = UUID.randomUUID();
    when(resultSet.getObject(1)).thenReturn(uuidExpected);
    final Array arrayMock = TestHelper.mockDbArray(Types.OTHER, "uuid", new UUID[] {uuidExpected});
    when(resultSet.getArray(2)).thenReturn(arrayMock);
    when(resultSet.isFirst()).thenReturn(true);

    String arrayMode = ArrayHandlingMode.TypedMetaFromFirstRow;
    final Schema schema = JdbcAvroSchema.createAvroSchema(resultSet, "ns", "conn_url",
        Optional.empty(), "doc", false, arrayMode, false);
    final JdbcAvroRecordConverter converter = JdbcAvroRecordConverter.create(
        resultSet, arrayMode, false);

    GenericRecord actualRecord = bytesToGenericRecords(schema,
        converter.convertResultSetIntoAvroBytes())[0];

    Assert.assertEquals(actualRecord.get("uuid_field"), new Utf8(uuidExpected.toString()));
    assertGenericRecordArrayField(actualRecord, "array_field", new Utf8(uuidExpected.toString()));
  }

  @Test
  public void shouldEncodeStringValues() throws SQLException, IOException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(3);
    TestHelper.mockResultSetMeta(meta, 1, Types.VARCHAR, "text_field", "java.lang.String", "text");

    final ResultSet resultSet = Mockito.mock(ResultSet.class);
    when(resultSet.getMetaData()).thenReturn(meta);
    when(resultSet.getString(1)).thenReturn("some_text_42");

    TestHelper.mockArrayColumn(meta, resultSet, 2, "array_field1", "_text", Types.VARCHAR,
        "text", new String[] {"some_text_42"});
    TestHelper.mockArrayColumn(meta, resultSet, 3, "array_field2", "_varchar",
        Types.VARCHAR, "varchar", (Object) new String[] {"some_varchar_42"});
    when(resultSet.isFirst()).thenReturn(true);

    String arrayMode = ArrayHandlingMode.TypedMetaFromFirstRow;
    final Schema schema = JdbcAvroSchema.createAvroSchema(resultSet, "ns", "conn_url",
        Optional.empty(), "doc", true, arrayMode, false);
    final JdbcAvroRecordConverter converter = JdbcAvroRecordConverter.create(resultSet, arrayMode,
        false);

    GenericRecord actualRecord = bytesToGenericRecords(schema,
        converter.convertResultSetIntoAvroBytes())[0];
    Assert.assertEquals(actualRecord.get("text_field"), new Utf8("some_text_42"));
    assertGenericRecordArrayField(actualRecord, "array_field1", "some_text_42");
    assertGenericRecordArrayField(actualRecord, "array_field2", "some_varchar_42");
  }

  @Test
  public void shouldThrowOnArrayWithNulls() throws SQLException, IOException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    TestHelper.mockResultSetMeta(meta, 1, Types.ARRAY, "array_field", "java.sql.Array", "_uuid");
    final ResultSet resultSet = Mockito.mock(ResultSet.class);
    when(resultSet.getMetaData()).thenReturn(meta);
    when(resultSet.getArray(1)).thenReturn(null);
    when(resultSet.isFirst()).thenReturn(true);

    Assert.assertThrows(RuntimeException.class, () -> JdbcAvroSchema.createAvroSchema(resultSet,
        "ns", "conn_url",
        Optional.empty(), "doc", true, ArrayHandlingMode.TypedMetaFromFirstRow, false));
  }

  @Test
  public void shouldHandleArrayWithNullsUsingArrayAsBytes() throws SQLException, IOException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    TestHelper.mockResultSetMeta(meta, 1, Types.ARRAY, "array_field", "java.sql.Array", "_uuid");
    final ResultSet resultSet = Mockito.mock(ResultSet.class);
    when(resultSet.getMetaData()).thenReturn(meta);
    when(resultSet.getArray(1)).thenReturn(null);
    byte[] expectedValue = new byte[] {1, 2, 3};
    when(resultSet.getBytes(1)).thenReturn(expectedValue);
    when(resultSet.isFirst()).thenReturn(true);
    String arrayMode = ArrayHandlingMode.Bytes;

    final Schema schema = JdbcAvroSchema.createAvroSchema(resultSet, "ns", "conn_url",
        Optional.empty(), "doc", true, arrayMode, false);
    final JdbcAvroRecordConverter converter = JdbcAvroRecordConverter.create(resultSet, arrayMode,
        false);

    GenericRecord actualRecord = bytesToGenericRecords(schema,
        converter.convertResultSetIntoAvroBytes())[0];
    Assert.assertArrayEquals(expectedValue,
        ((java.nio.ByteBuffer) actualRecord.get("array_field")).array());
  }

  @Test
  public void shouldHandleArrayWithNullsWithoutReadingFirstRow() throws SQLException, IOException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(3);
    final ResultSet resultSet = Mockito.mock(ResultSet.class);
    when(resultSet.getMetaData()).thenReturn(meta);

    TestHelper.mockArrayColumn(meta, resultSet, 1, "array_field_varchar", "_varchar",
        Types.VARCHAR, "varchar", null, (Object) new String[] {"some_varchar_42", "42"});
    TestHelper.mockArrayColumn(meta, resultSet, 2, "array_field_text", "_text",
        Types.VARCHAR, "text", null, (Object) new String[] {"some_text_42", "42"});
    final UUID uuidExpected = UUID.randomUUID();
    TestHelper.mockArrayColumn(meta, resultSet, 3, "array_field_uuid", "_uuid",
        Types.VARCHAR, "uuid", null, (Object) new UUID[] {uuidExpected});
    when(resultSet.next()).thenReturn(true, true, false);
    when(resultSet.isFirst()).thenReturn(true);
    String arrayMode = ArrayHandlingMode.TypedMetaPostgres;

    final Schema schema = JdbcAvroSchema.createAvroSchema(resultSet, "ns", "conn_url",
        Optional.empty(), "doc", true, arrayMode, false);
    final JdbcAvroRecordConverter converter = JdbcAvroRecordConverter.create(resultSet, arrayMode,
        false);
    GenericRecord[] actualRecords = bytesToGenericRecords(schema,
        converter.convertResultSetIntoAvroBytes(), converter.convertResultSetIntoAvroBytes());

    Assert.assertNull(actualRecords[0].get("array_field_varchar"));
    Assert.assertNull(actualRecords[0].get("array_field_text"));
    Assert.assertNull(actualRecords[0].get("array_field_uuid"));
    assertGenericRecordArrayField(actualRecords[1], "array_field_varchar", "some_varchar_42", "42");
    assertGenericRecordArrayField(actualRecords[1], "array_field_text", "some_text_42", "42");
    assertGenericRecordArrayField(actualRecords[1], "array_field_uuid",
        uuidExpected.toString());
  }

  @Test
  public void shouldHandleArrayWithNullsIfEnabled() throws SQLException, IOException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(3);
    final ResultSet resultSet = Mockito.mock(ResultSet.class);
    when(resultSet.getMetaData()).thenReturn(meta);

    TestHelper.mockArrayColumn(meta, resultSet, 1, "array_field_varchar", "_varchar",
        Types.VARCHAR, "varchar", new String[] { null, "some_varchar_42", "42"});
    TestHelper.mockArrayColumn(meta, resultSet, 2, "array_field_text", "_text",
        Types.VARCHAR, "text", new String[] { "some_text_42", null, "42"});
    final UUID uuidExpected = UUID.randomUUID();
    TestHelper.mockArrayColumn(meta, resultSet, 3, "array_field_uuid", "_uuid",
        Types.VARCHAR, "uuid", new UUID[] { uuidExpected, null});
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.isFirst()).thenReturn(true, false);
    String arrayMode = ArrayHandlingMode.TypedMetaPostgres;
    boolean nullableArrayItems = true;

    final Schema schema = JdbcAvroSchema.createAvroSchema(resultSet, "ns", "conn_url",
        Optional.empty(), "doc", true, arrayMode, nullableArrayItems);
    final JdbcAvroRecordConverter converter = JdbcAvroRecordConverter.create(resultSet, arrayMode,
        nullableArrayItems);
    GenericRecord actualRecord = bytesToGenericRecords(schema,
        converter.convertResultSetIntoAvroBytes(), converter.convertResultSetIntoAvroBytes())[0];

    assertGenericRecordArrayField(actualRecord, "array_field_varchar", null, "some_varchar_42",
        "42");
    assertGenericRecordArrayField(actualRecord, "array_field_text", "some_text_42", null, "42");
    assertGenericRecordArrayField(actualRecord, "array_field_uuid",
        uuidExpected.toString(), null);
  }

  @Test
  public void shouldThrowOnArrayWithNullsIfDisabled() throws SQLException, IOException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(3);
    final ResultSet resultSet = Mockito.mock(ResultSet.class);
    when(resultSet.getMetaData()).thenReturn(meta);

    TestHelper.mockArrayColumn(meta, resultSet, 1, "array_field_varchar", "_varchar",
        Types.VARCHAR, "varchar", new String[] { null, "some_varchar_42", "42"});
    TestHelper.mockArrayColumn(meta, resultSet, 2, "array_field_text", "_text",
        Types.VARCHAR, "text", new String[] { "some_text_42", null, "42"});
    final UUID uuidExpected = UUID.randomUUID();
    TestHelper.mockArrayColumn(meta, resultSet, 3, "array_field_uuid", "_uuid",
        Types.VARCHAR, "uuid", new UUID[] { uuidExpected, null});
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.isFirst()).thenReturn(true, false);
    String arrayMode = ArrayHandlingMode.TypedMetaPostgres;
    boolean nullableArrayItems = false;

    final Schema schema = JdbcAvroSchema.createAvroSchema(resultSet, "ns", "conn_url",
        Optional.empty(), "doc", true, arrayMode, nullableArrayItems);
    final JdbcAvroRecordConverter converter = JdbcAvroRecordConverter.create(resultSet, arrayMode,
        nullableArrayItems);
    RuntimeException thrown = Assert.assertThrows(RuntimeException.class,
        () -> converter.convertResultSetIntoAvroBytes());
    Assert.assertEquals("Array item is null in column array_field_varchar, use "
                        + "--nullableArrayItems", thrown.getMessage());
  }
}
