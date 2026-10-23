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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.UUID;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class JdbcParquetWriteSupportTest {

  private ResultSetMetaData meta;
  private ResultSet rs;
  private RecordConsumer consumer;

  @BeforeEach
  public void setUp() throws SQLException {
    meta = mock(ResultSetMetaData.class);
    rs = mock(ResultSet.class);
    consumer = mock(RecordConsumer.class);
    when(meta.getColumnName(1)).thenReturn("col");
    when(rs.wasNull()).thenReturn(false);
  }

  private void write(int sqlType, String arrayMode, Type fieldType) throws SQLException {
    when(meta.getColumnType(1)).thenReturn(sqlType);
    JdbcParquetWriteSupport.computeColumnWriter(meta, 1, arrayMode, fieldType, false)
        .write(consumer, rs);
  }

  private void writeTimestamp(final Type fieldType, final boolean micros) throws SQLException {
    final java.sql.Timestamp ts = new java.sql.Timestamp(1488300933123L);
    ts.setNanos(123456789);
    when(meta.getColumnType(1)).thenReturn(Types.TIMESTAMP);
    when(rs.getTimestamp(Mockito.eq(1), Mockito.any())).thenReturn(ts);
    JdbcParquetWriteSupport.computeColumnWriter(meta, 1, "bytes", fieldType, micros)
        .write(consumer, rs);
  }

  private static Type int64(final LogicalTypeAnnotation annotation) {
    return org.apache.parquet.schema.Types.optional(PrimitiveTypeName.INT64)
        .as(annotation)
        .named("col");
  }

  @Test
  public void shouldWriteTimestampUnitFromFlagNotAnnotation() throws SQLException {
    // the flag decides; the annotation is not consulted (the validator rejects mismatches)
    writeTimestamp(
        int64(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)),
        true);
    verify(consumer).addLong(1488300933123456L);
  }

  @Test
  public void shouldWriteUnannotatedTimestampAsMicrosWithFlag() throws SQLException {
    writeTimestamp(int64(null), true);
    verify(consumer).addLong(1488300933123456L);
  }

  @Test
  public void shouldWriteUnannotatedTimestampAsMillisWithoutFlag() throws SQLException {
    writeTimestamp(int64(null), false);
    verify(consumer).addLong(1488300933123L);
  }

  @Test
  public void shouldWriteIntegerAsLongWhenClassIsLong() throws SQLException {
    when(meta.getColumnClassName(1)).thenReturn("java.lang.Long");
    when(rs.getLong(1)).thenReturn(42L);
    write(Types.INTEGER, "bytes", null);
    verify(consumer).addLong(42L);
  }

  @Test
  public void shouldSkipNullIntegerAsLong() throws SQLException {
    when(meta.getColumnClassName(1)).thenReturn("java.lang.Long");
    when(rs.wasNull()).thenReturn(true);
    write(Types.INTEGER, "bytes", null);
    verify(consumer, never()).startField("col", 0);
  }

  @Test
  public void shouldWriteSingleBitAsBoolean() throws SQLException {
    when(meta.getPrecision(1)).thenReturn(1);
    when(rs.getBoolean(1)).thenReturn(true);
    write(Types.BIT, "bytes", null);
    verify(consumer).addBoolean(true);
  }

  @Test
  public void shouldWriteMultiBitAsBinary() throws SQLException {
    final byte[] bytes = {1, 2};
    when(meta.getPrecision(1)).thenReturn(8);
    when(rs.getBytes(1)).thenReturn(bytes);
    write(Types.BIT, "bytes", null);
    verify(consumer).addBinary(Binary.fromConstantByteArray(bytes));
  }

  @Test
  public void shouldWriteArrayAsBytesInBytesMode() throws SQLException {
    final byte[] bytes = {3, 4};
    when(rs.getBytes(1)).thenReturn(bytes);
    write(Types.ARRAY, "bytes", null);
    verify(consumer).addBinary(Binary.fromConstantByteArray(bytes));
  }

  @Test
  public void shouldWriteUuidWithLogicalType() throws SQLException {
    final UUID uuid = UUID.fromString("123e4567-e89b-12d3-a456-426655440000");
    final Type uuidType =
        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
            .length(16)
            .as(LogicalTypeAnnotation.uuidType())
            .named("col");
    when(meta.getColumnTypeName(1)).thenReturn("uuid");
    when(rs.getObject(1)).thenReturn(uuid.toString());
    write(Types.OTHER, "bytes", uuidType);
    final ByteBuffer buf = ByteBuffer.allocate(16);
    buf.putLong(uuid.getMostSignificantBits());
    buf.putLong(uuid.getLeastSignificantBits());
    verify(consumer).addBinary(Binary.fromConstantByteArray(buf.array()));
  }

  @Test
  public void shouldSkipNullUuid() throws SQLException {
    final Type uuidType =
        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
            .length(16)
            .as(LogicalTypeAnnotation.uuidType())
            .named("col");
    when(meta.getColumnTypeName(1)).thenReturn("uuid");
    write(Types.OTHER, "bytes", uuidType);
    verify(consumer, never()).startField("col", 0);
  }

  @Test
  public void shouldWriteTypedArrayElements() throws SQLException {
    final Array array = mock(Array.class);
    when(meta.getColumnTypeName(1)).thenReturn("_int8");
    when(rs.getArray(1)).thenReturn(array);
    when(array.getArray()).thenReturn(new Object[] {7L, null});
    write(Types.ARRAY, "typed_postgres", null);
    verify(consumer).addLong(7L);
    // LIST group + one group per element (null element still gets an empty group)
    verify(consumer, Mockito.times(3)).startGroup();
    verify(consumer, Mockito.times(1)).startField("element", 0);
  }

  @Test
  public void shouldFailClearlyOnMultidimensionalArrays() throws SQLException {
    final Object[][] cases = {
      {"_int2", new Short[][] {{1, 2}}},
      {"_int4", new Integer[][] {{1, 2}, {3, 4}}},
      {"_int8", new Long[][] {{1L}}},
      {"_float4", new Float[][] {{1f}}},
      {"_float8", new Double[][] {{1d}}},
      {"_bool", new Boolean[][] {{true}}},
      {"_bytea", new byte[][][] {{{1, 2}}, {{3}}}},
      {"_text", new String[][] {{"a", "b"}}},
      {"_varchar", new String[][] {{"a"}}},
      {"_bpchar", new String[][] {{"a"}}},
      {"_uuid", new java.util.UUID[][] {{java.util.UUID.randomUUID()}}},
      {"_int4", new Object[] {mock(Array.class)}},
      {"_int4", new Object[] {new int[] {1}}},
      {"_text", new Object[] {new byte[] {1}}},
    };
    for (Object[] c : cases) {
      for (String arrayMode : new String[] {"typed_first_row", "typed_postgres"}) {
        final RecordConsumer recordConsumer = mock(RecordConsumer.class);
        final Array array = mock(Array.class);
        when(meta.getColumnTypeName(1)).thenReturn((String) c[0]);
        when(meta.getColumnType(1)).thenReturn(Types.ARRAY);
        when(rs.getArray(1)).thenReturn(array);
        when(array.getArray()).thenReturn(c[1]);
        final JdbcParquetWriteSupport.ColumnWriter writer =
            JdbcParquetWriteSupport.computeColumnWriter(meta, 1, arrayMode, null, false);
        final IllegalArgumentException e =
            Assertions.assertThrows(
                IllegalArgumentException.class, () -> writer.write(recordConsumer, rs));
        Assertions.assertEquals(
            "Column 'col' contains a multidimensional array, which is not supported;"
                + " use --arrayMode=bytes to export the column as bytes",
            e.getMessage(),
            (String) c[0]);
        // nothing (in particular no "[Ljava.lang.String;@..." toString garbage) is written
        Mockito.verifyNoInteractions(recordConsumer);
      }
    }
  }

  @Test
  public void shouldWriteOneDimensionalByteaArray() throws SQLException {
    final Array array = mock(Array.class);
    final byte[] bytes = {1, 2};
    when(meta.getColumnTypeName(1)).thenReturn("_bytea");
    when(rs.getArray(1)).thenReturn(array);
    when(array.getArray()).thenReturn(new byte[][] {bytes});
    write(Types.ARRAY, "typed_postgres", null);
    verify(consumer).addBinary(Binary.fromConstantByteArray(bytes));
  }

  @Test
  public void shouldWriteArrayElementsByType() {
    JdbcParquetWriteSupport.writeArrayElement(consumer, 1.5f, "float4");
    JdbcParquetWriteSupport.writeArrayElement(consumer, 2.5d, "float8");
    JdbcParquetWriteSupport.writeArrayElement(consumer, true, "bool");
    JdbcParquetWriteSupport.writeArrayElement(consumer, 3, "int2");
    JdbcParquetWriteSupport.writeArrayElement(consumer, "x", "text");
    verify(consumer).addFloat(1.5f);
    verify(consumer).addDouble(2.5d);
    verify(consumer).addBoolean(true);
    verify(consumer).addInteger(3);
    verify(consumer).addBinary(Binary.fromString("x"));
  }

  @Test
  public void shouldWriteByteaArrayElementsAsBinary() {
    final byte[] bytes = {5, 6};
    JdbcParquetWriteSupport.writeArrayElement(consumer, bytes, "bytea");
    verify(consumer).addBinary(Binary.fromConstantByteArray(bytes));
    Assertions.assertEquals(
        PrimitiveTypeName.BINARY,
        JdbcParquetSchema.buildArrayElementType("arr", "_bytea")
            .asPrimitiveType()
            .getPrimitiveTypeName());
  }

  @Test
  public void shouldRejectUnsupportedArrayElementTypeInsteadOfToString() {
    final IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                JdbcParquetWriteSupport.writeArrayElement(
                    consumer, new java.sql.Timestamp(0), "timestamp"));
    Assertions.assertTrue(e.getMessage().contains("'timestamp'"), e.getMessage());
    Assertions.assertTrue(e.getMessage().contains("--arrayMode=bytes"), e.getMessage());
    Mockito.verifyNoInteractions(consumer);
  }

  @Test
  public void shouldWriteUnsupportedArrayElementAsStringOnlyForStringSchemaElement() {
    final org.apache.parquet.schema.Type stringList =
        org.apache.parquet.schema.Types.optionalList()
            .element(JdbcParquetSchema.buildArrayElementType("arr", "_timestamp", false, true))
            .named("arr");
    Assertions.assertEquals(
        "text", JdbcParquetWriteSupport.writtenArrayElementType("timestamp", stringList));
    final org.apache.parquet.schema.Type intList =
        org.apache.parquet.schema.Types.optionalList()
            .element(JdbcParquetSchema.buildArrayElementType("arr", "_int4", false))
            .named("arr");
    Assertions.assertEquals(
        "timestamp", JdbcParquetWriteSupport.writtenArrayElementType("timestamp", intList));
    Assertions.assertEquals(
        "int4", JdbcParquetWriteSupport.writtenArrayElementType("int4", stringList));
  }

  @Test
  public void shouldOmitFooterAvroSchemaWhenGenerationFails() {
    Assertions.assertNull(
        JdbcParquetJob.footerAvroSchema(
            () -> {
              throw new SQLException("boom");
            }));
  }

  @Test
  public void shouldKeepFormatSpecificMetricNamespaces() {
    Assertions.assertEquals(
        "com.spotify.dbeam.avro.JdbcAvroMetering",
        com.spotify.dbeam.avro.JdbcAvroMetering.create().getClass().getCanonicalName());
    Assertions.assertEquals(
        "com.spotify.dbeam.parquet.JdbcParquetMetering",
        JdbcParquetMetering.create().getClass().getCanonicalName());
  }

  @Test
  public void shouldRescaleDecimals() {
    Assertions.assertEquals(
        java.math.BigInteger.valueOf(15000),
        JdbcParquetWriteSupport.unscaledDecimal(new java.math.BigDecimal("1.5"), 6, 4, "c"));
    Assertions.assertEquals(
        java.math.BigInteger.valueOf(-99999),
        JdbcParquetWriteSupport.unscaledDecimal(new java.math.BigDecimal("-999.99"), 5, 2, "c"));
  }

  @Test
  public void shouldFailOnDecimalOverflowOrRounding() {
    final IllegalArgumentException precision =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                JdbcParquetWriteSupport.unscaledDecimal(
                    new java.math.BigDecimal("1000.00"), 5, 2, "price"));
    Assertions.assertTrue(
        precision.getMessage().contains("exceeds the precision of DECIMAL(5,2)"),
        precision.getMessage());
    final IllegalArgumentException rounding =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                JdbcParquetWriteSupport.unscaledDecimal(
                    new java.math.BigDecimal("1.234"), 5, 2, "price"));
    Assertions.assertTrue(
        rounding.getMessage().contains("more than 2 fractional digits"), rounding.getMessage());
  }

  @Test
  public void shouldEncodeFixedTwosComplement() {
    Assertions.assertArrayEquals(
        new byte[] {0, 0, 1},
        JdbcParquetWriteSupport.toFixed(java.math.BigInteger.ONE, 3));
    Assertions.assertArrayEquals(
        new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF},
        JdbcParquetWriteSupport.toFixed(java.math.BigInteger.ONE.negate(), 3));
    Assertions.assertArrayEquals(
        new byte[] {(byte) 0xFF, (byte) 0x7F},
        JdbcParquetWriteSupport.toFixed(java.math.BigInteger.valueOf(-129), 2));
  }
}
