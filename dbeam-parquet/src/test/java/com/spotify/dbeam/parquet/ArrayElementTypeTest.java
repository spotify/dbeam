/*-
 * -\-\-
 * DBeam Parquet
 * --
 * Copyright (C) 2016 - 2026 Spotify AB
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.spotify.dbeam.options.ArrayHandlingMode;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Optional;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** ARRAY element type resolution shared by the Parquet schema and writer. */
public class ArrayElementTypeTest {

  @Test
  public void shouldResolveFloatArraysBySqlPrecision() {
    // unsized FLOAT is double precision (H2, HSQLDB, SQL standard); FLOAT(p) single for p <= 24
    Assertions.assertEquals("float8", JdbcParquetSchema.resolveArrayElementTypeName("FLOAT ARRAY"));
    Assertions.assertEquals(
        "float4", JdbcParquetSchema.resolveArrayElementTypeName("FLOAT(10) ARRAY"));
    Assertions.assertEquals(
        "float4", JdbcParquetSchema.resolveArrayElementTypeName("FLOAT(24) ARRAY"));
    Assertions.assertEquals(
        "float8", JdbcParquetSchema.resolveArrayElementTypeName("FLOAT(25) ARRAY"));
    Assertions.assertEquals(
        "float8", JdbcParquetSchema.resolveArrayElementTypeName("FLOAT (53) ARRAY"));
    Assertions.assertEquals("float4", JdbcParquetSchema.resolveArrayElementTypeName("REAL ARRAY"));
    Assertions.assertEquals(
        "float8", JdbcParquetSchema.resolveArrayElementTypeName("DOUBLE PRECISION ARRAY"));
    Assertions.assertEquals("float4", JdbcParquetSchema.resolveArrayElementTypeName("_float4"));
    Assertions.assertEquals("float8", JdbcParquetSchema.resolveArrayElementTypeName("_float8"));
    // base type names of PostgreSQL arrays (typed_first_row)
    Assertions.assertEquals("float4", JdbcParquetSchema.resolveArrayElementTypeName("float4"));
    Assertions.assertEquals("float8", JdbcParquetSchema.resolveArrayElementTypeName("float8"));
  }

  @Test
  public void shouldWriteUnsizedFloatArrayAsDouble() throws SQLException {
    final MessageType schema =
        JdbcParquetSchema.createParquetSchema(
            arrayResultSet("FLOAT ARRAY", null), Optional.empty(), postgresMode());
    Assertions.assertEquals(PrimitiveTypeName.DOUBLE, elementType(schema).getPrimitiveTypeName());

    final ResultSet values = arrayResultSet("FLOAT ARRAY", new Object[] {0.1d, 1e300d});
    final RecordConsumer consumer = writeRow(values, schema, postgresMode());
    verify(consumer).addDouble(0.1d);
    verify(consumer).addDouble(1e300d);
  }

  @Test
  public void shouldResolveFirstRowArrayElementType() throws SQLException {
    Assertions.assertNull(JdbcParquetSchema.firstRowArrayElementType(null));
    Assertions.assertEquals("int4", firstRow("int4", Types.INTEGER));
    Assertions.assertEquals("int8", firstRow("BIGINT", Types.BIGINT));
    Assertions.assertEquals("text", firstRow("CHARACTER VARYING", Types.VARCHAR));
    // unknown or missing base type name: from the JDBC base type
    Assertions.assertEquals("int8", firstRow("mystery_int", Types.BIGINT));
    Assertions.assertEquals("float8", firstRow(null, Types.DOUBLE));
    Assertions.assertEquals("float4", firstRow(null, Types.REAL));
    Assertions.assertEquals("bool", firstRow(null, Types.BOOLEAN));
    Assertions.assertEquals("bytea", firstRow(null, Types.VARBINARY));
    Assertions.assertEquals("text", firstRow(null, Types.CLOB));
    Assertions.assertEquals("mystery", firstRow("mystery", Types.OTHER));
  }

  @Test
  public void shouldTypeArrayFromFirstRowWhenColumnTypeNameIsGeneric() throws SQLException {
    // e.g. a driver reporting only "ARRAY" as the column type name
    final Array firstRow = array("BIGINT", Types.BIGINT, new Object[] {1L, 2L});
    final ParquetSchemaOptions firstRowMode =
        ParquetSchemaOptions.of(ArrayHandlingMode.TypedMetaFromFirstRow);
    final MessageType schema =
        JdbcParquetSchema.createParquetSchema(
            arrayResultSet("ARRAY", firstRow), Optional.empty(), firstRowMode);
    Assertions.assertEquals(PrimitiveTypeName.INT64, elementType(schema).getPrimitiveTypeName());

    // the writer follows the schema's element type, not the column type name
    final RecordConsumer consumer =
        writeRow(arrayResultSet("ARRAY", firstRow), schema, firstRowMode);
    verify(consumer).addLong(1L);
    verify(consumer).addLong(2L);

    // typed_postgres only has the column type name to go by
    final IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                JdbcParquetSchema.createParquetSchema(
                    arrayResultSet("ARRAY", firstRow), Optional.empty(), postgresMode()));
    Assertions.assertTrue(e.getMessage().contains("ARRAY element type 'array'"), e.getMessage());
  }

  @Test
  public void shouldFallBackToColumnTypeNameForNullFirstRow() throws SQLException {
    final MessageType schema =
        JdbcParquetSchema.createParquetSchema(
            arrayResultSet("INTEGER ARRAY", null),
            Optional.empty(),
            ParquetSchemaOptions.of(ArrayHandlingMode.TypedMetaFromFirstRow));
    Assertions.assertEquals(PrimitiveTypeName.INT32, elementType(schema).getPrimitiveTypeName());
  }

  @Test
  public void shouldCheckNestingOnFirstNonNullItemOnly() {
    final IllegalArgumentException nested =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                JdbcParquetWriteSupport.checkArrayItems(
                    new Object[] {null, new Integer[] {1}}, "int4", true, "matrix"));
    Assertions.assertTrue(nested.getMessage().contains("multidimensional"), nested.getMessage());

    final RuntimeException nullItem =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                JdbcParquetWriteSupport.checkArrayItems(
                    new Object[] {1, 2, null}, "int4", false, "arr"));
    Assertions.assertEquals(
        "Array item is null in column 'arr', use --nullableArrayItems", nullItem.getMessage());

    // bytea elements are byte[], not nested arrays
    JdbcParquetWriteSupport.checkArrayItems(
        new Object[] {new byte[] {1}, null}, "bytea", true, "bytes");
    JdbcParquetWriteSupport.checkArrayItems(new Object[] {}, "int4", false, "empty");
  }

  private static ParquetSchemaOptions postgresMode() {
    return ParquetSchemaOptions.of(ArrayHandlingMode.TypedMetaPostgres);
  }

  private static String firstRow(final String baseTypeName, final int baseType)
      throws SQLException {
    return JdbcParquetSchema.firstRowArrayElementType(
        array(baseTypeName, baseType, new Object[] {}));
  }

  private static Array array(final String baseTypeName, final int baseType, final Object[] items)
      throws SQLException {
    final Array array = mock(Array.class);
    when(array.getBaseTypeName()).thenReturn(baseTypeName);
    when(array.getBaseType()).thenReturn(baseType);
    when(array.getArray()).thenReturn(items);
    return array;
  }

  /** One ARRAY column {@code arr}, positioned on a first row holding {@code value}. */
  private static ResultSet arrayResultSet(final String columnTypeName, final Object value)
      throws SQLException {
    final ResultSetMetaData meta = mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getColumnName(1)).thenReturn("arr");
    when(meta.getColumnType(1)).thenReturn(Types.ARRAY);
    when(meta.getColumnTypeName(1)).thenReturn(columnTypeName);
    when(meta.getColumnClassName(1)).thenReturn("java.sql.Array");
    final ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getMetaData()).thenReturn(meta);
    when(resultSet.isFirst()).thenReturn(true);
    final Array array =
        value instanceof Object[] ? array(null, Types.OTHER, (Object[]) value) : (Array) value;
    when(resultSet.getArray(1)).thenReturn(array);
    when(resultSet.wasNull()).thenReturn(array == null);
    return resultSet;
  }

  private static RecordConsumer writeRow(
      final ResultSet resultSet, final MessageType schema, final ParquetSchemaOptions options)
      throws SQLException {
    final RecordConsumer consumer = mock(RecordConsumer.class);
    JdbcParquetWriteSupport.create(resultSet, schema, options).writeRecord(consumer, resultSet);
    return consumer;
  }

  private static org.apache.parquet.schema.PrimitiveType elementType(final MessageType schema) {
    final Type element = schema.getType(0).asGroupType().getType(0).asGroupType().getType(0);
    return element.asPrimitiveType();
  }
}
