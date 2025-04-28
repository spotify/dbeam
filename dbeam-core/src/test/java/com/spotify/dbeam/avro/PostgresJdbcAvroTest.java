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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
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

//  public static GenericRecord readFirstGenericRecord(byte[] avroDocBytes, Schema schema)
//      throws IOException {
//    SeekableByteArrayInput inputStream = new SeekableByteArrayInput(avroDocBytes);
//    DataFileReader<GenericRecord> dataReader = new DataFileReader<>(inputStream,
//        new GenericDatumReader<>(schema));
//    return dataReader.next();
//  }

  public static GenericRecord bytesToGenericRecord(Schema schema,
                                                             ByteBuffer avroRecordBytes)
      throws IOException {
    DataFileWriter<GenericRecord> dataFileWriter =
        new DataFileWriter<>(new GenericDatumWriter<>(schema));
    ByteArrayOutputStream avroOutputStream = new ByteArrayOutputStream();
    dataFileWriter.create(schema, avroOutputStream);
    dataFileWriter.appendEncoded(avroRecordBytes);
    dataFileWriter.close();

    SeekableByteArrayInput inputStream = new SeekableByteArrayInput(avroOutputStream.toByteArray());
    DataFileReader<GenericRecord> dataReader = new DataFileReader<>(inputStream,
        new GenericDatumReader<>(schema));
    return dataReader.next();
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

    final Schema schema = JdbcAvroSchema.createAvroSchema(resultSet, "ns", "conn_url",
        Optional.empty(), "doc", true, false);
    final JdbcAvroRecordConverter converter = JdbcAvroRecordConverter.create(resultSet, false);

    GenericRecord actualRecord = bytesToGenericRecord(schema, converter.convertResultSetIntoAvroBytes());
    Assert.assertEquals(actualRecord.get("uuid_field"), new Utf8(uuidExpected.toString()));
    final GenericData.Array<GenericRecord> arrayValue = (GenericData.Array<GenericRecord>)actualRecord.get("array_field");
    Assert.assertEquals(arrayValue.size(), 1);
    Assert.assertEquals(arrayValue.get(0), new Utf8(uuidExpected.toString()));
  }

  @Test
  public void shouldEncodeStringValues() throws SQLException, IOException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(3);
    TestHelper.mockResultSetMeta(meta, 1, Types.VARCHAR, "text_field", "java.lang.String", "text");
    TestHelper.mockResultSetMeta(meta, 2, Types.ARRAY, "array_field1", "java.sql.Array", "_text");
    TestHelper.mockResultSetMeta(meta, 3, Types.ARRAY, "array_field2", "java.sql.Array",
        "_varchar");

    final ResultSet resultSet = Mockito.mock(ResultSet.class);
    when(resultSet.getMetaData()).thenReturn(meta);
    when(resultSet.getString(1)).thenReturn("some_text_42");
    final Array arrayMock1 = TestHelper.mockDbArray(Types.VARCHAR, "text",
        new String[] {"some_text_42"});
    when(resultSet.getArray(2)).thenReturn(arrayMock1);
    final Array arrayMock2 = TestHelper.mockDbArray(Types.VARCHAR, "uuid",
        new String[] {"some_varchar_42"});
    when(resultSet.getArray(3)).thenReturn(arrayMock2);
    when(resultSet.isFirst()).thenReturn(true);

    final Schema schema = JdbcAvroSchema.createAvroSchema(resultSet, "ns", "conn_url",
        Optional.empty(), "doc", true, false);
    final JdbcAvroRecordConverter converter = JdbcAvroRecordConverter.create(resultSet, false);

    GenericRecord actualRecord = bytesToGenericRecord(schema, converter.convertResultSetIntoAvroBytes());
    Assert.assertEquals(actualRecord.get("text_field"), new Utf8("some_text_42"));
    final GenericData.Array<GenericRecord> arrayValue1 =
        (GenericData.Array<GenericRecord>)actualRecord.get("array_field1");
    Assert.assertEquals(arrayValue1.size(), 1);
    Assert.assertEquals(arrayValue1.get(0), new Utf8("some_text_42"));
    final GenericData.Array<GenericRecord> arrayValue2 =
        (GenericData.Array<GenericRecord>)actualRecord.get("array_field2");
    Assert.assertEquals(arrayValue2.size(), 1);
    Assert.assertEquals(arrayValue2.get(0), new Utf8("some_varchar_42"));
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

    Assert.assertThrows(RuntimeException.class, () -> JdbcAvroSchema.createAvroSchema(resultSet, "ns", "conn_url",
        Optional.empty(), "doc", true, false));
  }

  @Test
  public void shouldHandleArrayWithNulls() throws SQLException, IOException {
    boolean arrayAsBytes = true;
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    TestHelper.mockResultSetMeta(meta, 1, Types.ARRAY, "array_field", "java.sql.Array", "_uuid");
    final ResultSet resultSet = Mockito.mock(ResultSet.class);
    when(resultSet.getMetaData()).thenReturn(meta);
    when(resultSet.getArray(1)).thenReturn(null);
    byte[] expectedValue = new byte[] {1, 2, 3};
    when(resultSet.getBytes(1)).thenReturn(expectedValue);
    when(resultSet.isFirst()).thenReturn(true);

    final Schema schema = JdbcAvroSchema.createAvroSchema(resultSet, "ns", "conn_url", Optional.empty(), "doc", true, arrayAsBytes);
    final JdbcAvroRecordConverter converter = JdbcAvroRecordConverter.create(resultSet, arrayAsBytes);
    GenericRecord actualRecord = bytesToGenericRecord(schema, converter.convertResultSetIntoAvroBytes());
    Assert.assertArrayEquals(expectedValue, ((java.nio.ByteBuffer)actualRecord.get("array_field")).array());
  }
}
