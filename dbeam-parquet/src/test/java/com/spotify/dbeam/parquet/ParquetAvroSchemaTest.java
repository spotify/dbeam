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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ParquetAvroSchemaTest {

  private static final UUID UUID_VALUE = UUID.fromString("123e4567-e89b-12d3-a456-426655440000");

  private static Schema nullable(final Schema schema) {
    return Schema.createUnion(Schema.create(Schema.Type.NULL), schema);
  }

  private static Schema convert(final String messageType) {
    return ParquetAvroSchema.toAvroSchema(
        MessageTypeParser.parseMessageType(messageType), "ns", "doc");
  }

  @Test
  public void shouldConvertPrimitives() {
    final Schema schema =
        convert(
            "message t { required int32 i; optional int64 l; optional boolean b; optional float f;"
                + " optional double d; optional binary s (STRING); optional binary raw;"
                + " optional int64 ts (TIMESTAMP(MILLIS,true));"
                + " optional int64 tsu (TIMESTAMP(MICROS,true));"
                + " optional int64 lts (TIMESTAMP(MICROS,false)); optional int32 dt (DATE);"
                + " optional int32 tm (TIME(MILLIS,true)); optional int64 tmu (TIME(MICROS,true));"
                + " optional binary dec (DECIMAL(10,2));"
                + " optional fixed_len_byte_array(16) u (UUID);"
                + " optional fixed_len_byte_array(4) fx; }");
    Assertions.assertEquals("t", schema.getName());
    Assertions.assertEquals("ns", schema.getNamespace());
    Assertions.assertEquals("doc", schema.getDoc());
    Assertions.assertEquals(Schema.create(Schema.Type.INT), schema.getField("i").schema());
    Assertions.assertFalse(schema.getField("i").hasDefaultValue());
    Assertions.assertEquals(
        nullable(Schema.create(Schema.Type.LONG)), schema.getField("l").schema());
    Assertions.assertTrue(schema.getField("l").hasDefaultValue());
    Assertions.assertEquals(
        nullable(Schema.create(Schema.Type.STRING)), schema.getField("s").schema());
    Assertions.assertEquals(
        nullable(Schema.create(Schema.Type.BYTES)), schema.getField("raw").schema());
    Assertions.assertEquals(
        nullable(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))),
        schema.getField("ts").schema());
    Assertions.assertEquals(
        nullable(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))),
        schema.getField("tsu").schema());
    Assertions.assertEquals(
        nullable(
            LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG))),
        schema.getField("lts").schema());
    Assertions.assertEquals(
        nullable(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))),
        schema.getField("dt").schema());
    Assertions.assertEquals(
        nullable(LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT))),
        schema.getField("tm").schema());
    Assertions.assertEquals(
        nullable(LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG))),
        schema.getField("tmu").schema());
    Assertions.assertEquals(
        nullable(LogicalTypes.decimal(10, 2).addToSchema(Schema.create(Schema.Type.BYTES))),
        schema.getField("dec").schema());
    Assertions.assertEquals(
        nullable(LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING))),
        schema.getField("u").schema());
    Assertions.assertEquals(
        nullable(Schema.createFixed("fx", null, null, 4)), schema.getField("fx").schema());
  }

  @Test
  public void shouldConvertListsAndNestedGroups() {
    final Schema schema =
        convert(
            "message t { optional group a (LIST) { repeated group list {"
                + " optional int32 element; } }"
                + " optional group r (LIST) { repeated group list { required binary element"
                + " (STRING); } }"
                + " optional group legacy (LIST) { repeated int32 array; }"
                + " optional group m (MAP) { repeated group key_value { required binary key"
                + " (STRING); optional int64 value; } }"
                + " optional group s { optional int32 x; } repeated int32 rep; }");
    Assertions.assertEquals(
        nullable(Schema.createArray(nullable(Schema.create(Schema.Type.INT)))),
        schema.getField("a").schema());
    Assertions.assertEquals(
        nullable(Schema.createArray(Schema.create(Schema.Type.STRING))),
        schema.getField("r").schema());
    Assertions.assertEquals(
        nullable(Schema.createArray(Schema.create(Schema.Type.INT))),
        schema.getField("legacy").schema());
    Assertions.assertEquals(
        nullable(Schema.createMap(nullable(Schema.create(Schema.Type.LONG)))),
        schema.getField("m").schema());
    Assertions.assertEquals(Schema.Type.RECORD, schema.getField("s").schema().getTypes().get(1)
        .getType());
    Assertions.assertEquals(
        Schema.createArray(Schema.create(Schema.Type.INT)), schema.getField("rep").schema());
    // must be a valid, parseable schema
    Assertions.assertEquals(schema, new Schema.Parser().parse(schema.toString()));
  }

  @Test
  public void shouldKeepNamedTypesUnique() {
    final Schema schema =
        convert(
            "message t { optional group t { optional fixed_len_byte_array(2) x; }"
                + " optional fixed_len_byte_array(2) x; }");
    Assertions.assertEquals(schema, new Schema.Parser().parse(schema.toString()));
  }

  @Test
  public void shouldRejectInt96() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> convert("message t { optional int96 x; }"));
    Assertions.assertNull(
        JdbcParquetJob.footerAvroSchema(() -> convert("message t { optional int96 x; }")));
  }

  @Test
  public void shouldSetNameNamespaceAndDocOnTopLevelRecordOnly() {
    final Schema schema =
        convert("message my_table { optional group s { optional int32 x; } required int32 i; }");
    Assertions.assertEquals("my_table", schema.getName());
    Assertions.assertEquals("ns.my_table", schema.getFullName());
    Assertions.assertEquals("doc", schema.getDoc());
    Assertions.assertTrue(schema.getObjectProps().isEmpty());
    for (Schema.Field field : schema.getFields()) {
      Assertions.assertNull(field.doc());
      Assertions.assertTrue(field.getObjectProps().isEmpty());
    }
    Assertions.assertNull(schema.getField("s").schema().getTypes().get(1).getDoc());
    Assertions.assertEquals(schema, new Schema.Parser().parse(schema.toString()));
  }

  @Test
  public void shouldAllowMissingNamespaceAndDoc() {
    final Schema schema =
        ParquetAvroSchema.toAvroSchema(
            MessageTypeParser.parseMessageType("message t { optional int32 i; }"), null, null);
    Assertions.assertEquals("t", schema.getFullName());
    Assertions.assertNull(schema.getDoc());
  }

  @Test
  public void shouldConvertArrayColumnWithoutListWrapperRecord() throws SQLException {
    final ResultSet rs = arrayResultSet("_int8", new Long[] {1L});
    final MessageType parquet =
        JdbcParquetSchema.createParquetSchema(rs, Optional.of("t"), true, "typed_postgres");
    final Schema arr = convert(parquet.toString()).getField("arr").schema();
    // required elements (no --nullableArrayItems): array<long>
    Assertions.assertEquals(nullable(Schema.createArray(Schema.create(Schema.Type.LONG))), arr);
    Assertions.assertFalse(arr.toString().contains("\"record\""));
    final Schema nullableItems =
        convert(nullableItemsSchema(rs, true, "typed_postgres").toString())
            .getField("arr")
            .schema();
    // optional elements (--nullableArrayItems): array<["null", long]>
    Assertions.assertEquals(
        nullable(Schema.createArray(nullable(Schema.create(Schema.Type.LONG)))), nullableItems);
  }

  private static MessageType nullableItemsSchema(
      final ResultSet rs, final boolean useLogicalTypes, final String arrayMode)
      throws SQLException {
    return JdbcParquetSchema.createParquetSchema(
        rs, Optional.empty(), useLogicalTypes, false, arrayMode, false, true);
  }

  @Test
  public void shouldRejectInvalidAvroNames() {
    Assertions.assertNull(
        JdbcParquetJob.footerAvroSchema(() -> convert("message t { optional int32 `a-b`; }")));
  }

  // ---- round trips through AvroParquetReader with the footer schema (mocked PostgreSQL) ----

  private static List<GenericRecord> writeAndRead(
      final MessageType schema, final ResultSet resultSet, final String arrayMode, final int rows)
      throws IOException, SQLException {
    final Schema avro =
        ParquetAvroSchema.toAvroSchema(schema, "ns", "doc");
    final Path tempFile = Files.createTempFile("parquet-footer-test-", ".parquet");
    Files.delete(tempFile);
    try (ParquetWriter<ResultSet> writer =
        new JdbcParquetIO.ResultSetParquetWriterBuilder(
                new ChannelOutputFile(
                    FileChannel.open(
                        tempFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE)),
                schema,
                avro.toString(),
                arrayMode,
                false)
            .withWriteMode(ParquetFileWriter.Mode.CREATE)
            .build()) {
      for (int i = 0; i < rows; i++) {
        writer.write(resultSet);
      }
    }
    final List<GenericRecord> records = new ArrayList<>();
    try (ParquetReader<GenericRecord> reader =
        AvroParquetReader.<GenericRecord>builder(
                HadoopInputFile.fromPath(
                    new org.apache.hadoop.fs.Path(tempFile.toUri()), new Configuration()))
            .build()) {
      GenericRecord record;
      while ((record = reader.read()) != null) {
        Assertions.assertEquals(avro, record.getSchema());
        records.add(record);
      }
    } finally {
      Files.deleteIfExists(tempFile);
    }
    return records;
  }

  private static ResultSet uuidResultSet() throws SQLException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn("t");
    TestHelper.mockResultSetMeta(meta, 1, Types.OTHER, "id", "java.util.UUID", "uuid");
    final ResultSet rs = Mockito.mock(ResultSet.class);
    when(rs.getMetaData()).thenReturn(meta);
    when(rs.getObject(1)).thenReturn(UUID_VALUE, (Object) null);
    when(rs.wasNull()).thenReturn(false, true);
    return rs;
  }

  @Test
  public void shouldRoundTripUuidWithLogicalTypes() throws Exception {
    final ResultSet rs = uuidResultSet();
    final MessageType schema =
        JdbcParquetSchema.createParquetSchema(rs, Optional.empty(), true, "typed_first_row");
    final List<GenericRecord> records = writeAndRead(schema, rs, "typed_first_row", 2);
    Assertions.assertEquals(
        "uuid", records.get(0).getSchema().getField("id").schema().getTypes().get(1)
            .getProp("logicalType"));
    Assertions.assertEquals(UUID_VALUE.toString(), records.get(0).get("id").toString());
    Assertions.assertNull(records.get(1).get("id"));
  }

  @Test
  public void shouldRoundTripUuidWithoutLogicalTypes() throws Exception {
    final ResultSet rs = uuidResultSet();
    final MessageType schema =
        JdbcParquetSchema.createParquetSchema(rs, Optional.empty(), false, "typed_first_row");
    final List<GenericRecord> records = writeAndRead(schema, rs, "typed_first_row", 2);
    Assertions.assertEquals(
        nullable(Schema.create(Schema.Type.STRING)),
        records.get(0).getSchema().getField("id").schema());
    Assertions.assertEquals(UUID_VALUE.toString(), records.get(0).get("id").toString());
    Assertions.assertNull(records.get(1).get("id"));
  }

  private static ResultSet arrayResultSet(final String typeName, final Object[] values)
      throws SQLException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn("t");
    TestHelper.mockResultSetMeta(meta, 1, Types.ARRAY, "arr", "java.sql.Array", typeName);
    final ResultSet rs = Mockito.mock(ResultSet.class);
    when(rs.getMetaData()).thenReturn(meta);
    final java.sql.Array array = Mockito.mock(java.sql.Array.class);
    when(array.getArray()).thenReturn(values);
    when(rs.getArray(1)).thenReturn(array, (java.sql.Array) null);
    when(rs.wasNull()).thenReturn(false, true);
    return rs;
  }

  private static List<Object> items(final GenericRecord record) {
    return ((List<?>) record.get("arr"))
        .stream()
            .map(o -> o instanceof CharSequence ? o.toString() : o)
            .collect(Collectors.toCollection(ArrayList::new));
  }

  @Test
  public void shouldRoundTripTextArrayWithNullElements() throws Exception {
    for (String mode : new String[] {"typed_first_row", "typed_postgres"}) {
      final ResultSet rs = arrayResultSet("_text", new String[] {"a", null, "c"});
      final MessageType schema = nullableItemsSchema(rs, true, mode);
      final List<GenericRecord> records = writeAndRead(schema, rs, mode, 2);
      Assertions.assertEquals(Arrays.asList("a", null, "c"), items(records.get(0)));
      Assertions.assertNull(records.get(1).get("arr"));
    }
  }

  @Test
  public void shouldRoundTripIntArrayWithNullElements() throws Exception {
    final ResultSet rs = arrayResultSet("_int8", new Long[] {1L, null, 3L});
    final MessageType schema = nullableItemsSchema(rs, true, "typed_postgres");
    final List<GenericRecord> records = writeAndRead(schema, rs, "typed_postgres", 2);
    Assertions.assertEquals(
        nullable(Schema.createArray(nullable(Schema.create(Schema.Type.LONG)))),
        records.get(0).getSchema().getField("arr").schema());
    Assertions.assertEquals(Arrays.asList(1L, null, 3L), items(records.get(0)));
    Assertions.assertNull(records.get(1).get("arr"));
  }

  @Test
  public void shouldRoundTripIntArrayWithRequiredElements() throws Exception {
    final ResultSet rs = arrayResultSet("_int8", new Long[] {1L, 3L});
    final MessageType schema =
        JdbcParquetSchema.createParquetSchema(rs, Optional.empty(), true, "typed_postgres");
    final List<GenericRecord> records = writeAndRead(schema, rs, "typed_postgres", 2);
    Assertions.assertEquals(
        nullable(Schema.createArray(Schema.create(Schema.Type.LONG))),
        records.get(0).getSchema().getField("arr").schema());
    Assertions.assertEquals(Arrays.asList(1L, 3L), items(records.get(0)));
    Assertions.assertNull(records.get(1).get("arr"));
  }

  @Test
  public void shouldRejectNullElementWithRequiredElements() throws Exception {
    final ResultSet rs = arrayResultSet("_int8", new Long[] {1L, null, 3L});
    final MessageType schema =
        JdbcParquetSchema.createParquetSchema(rs, Optional.empty(), true, "typed_postgres");
    final RuntimeException e =
        Assertions.assertThrows(
            RuntimeException.class, () -> writeAndRead(schema, rs, "typed_postgres", 1));
    Throwable cause = e;
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }
    Assertions.assertEquals(
        "Array item is null in column 'arr', use --nullableArrayItems", cause.getMessage());
  }

  @Test
  public void shouldRoundTripArrayInBytesMode() throws Exception {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn("t");
    TestHelper.mockResultSetMeta(meta, 1, Types.ARRAY, "arr", "java.sql.Array", "_int4");
    final ResultSet rs = Mockito.mock(ResultSet.class);
    when(rs.getMetaData()).thenReturn(meta);
    when(rs.getBytes(1)).thenReturn(new byte[] {1, 2}, (byte[]) null);
    when(rs.wasNull()).thenReturn(false, true);
    final MessageType schema =
        JdbcParquetSchema.createParquetSchema(rs, Optional.empty(), true, "bytes");
    final List<GenericRecord> records = writeAndRead(schema, rs, "bytes", 2);
    Assertions.assertEquals(
        nullable(Schema.create(Schema.Type.BYTES)),
        records.get(0).getSchema().getField("arr").schema());
    Assertions.assertEquals(ByteBuffer.wrap(new byte[] {1, 2}), records.get(0).get("arr"));
    Assertions.assertNull(records.get(1).get("arr"));
  }
}
