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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Optional;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class JdbcParquetSchemaTest {

  public static final int COLUMN_NUM = 1;

  @Test
  public void shouldGetDatabaseTableNameFromMetaData() throws SQLException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn("test_table");

    Assertions.assertEquals("test_table", JdbcParquetSchema.getDatabaseTableName(meta));
  }

  @Test
  public void shouldDefaultTableNameWhenMetaDataHasEmptyTableName() throws SQLException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn("");

    Assertions.assertEquals("no_table_name", JdbcParquetSchema.getDatabaseTableName(meta));
  }

  @Test
  public void shouldDefaultTableNameWhenMetaDataHasNullTableName() throws SQLException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn(null);

    Assertions.assertEquals("no_table_name", JdbcParquetSchema.getDatabaseTableName(meta));
  }

  @Test
  public void shouldGetDatabaseTableNameFromFirstNonNullMetaData() throws SQLException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(2);
    when(meta.getTableName(1)).thenReturn("");
    when(meta.getTableName(2)).thenReturn("test_table");

    Assertions.assertEquals("test_table", JdbcParquetSchema.getDatabaseTableName(meta));
  }

  @Test
  public void shouldConvertBigIntSqlTypeToInt64() throws SQLException {
    final Type fieldType = buildFieldType(Types.BIGINT, false);

    assertPrimitiveType(fieldType, PrimitiveType.PrimitiveTypeName.INT64);
  }

  @Test
  public void shouldConvertIntegerSqlTypeToInt32() throws SQLException {
    final Type fieldType = buildFieldType(Types.INTEGER, false);

    assertPrimitiveType(fieldType, PrimitiveType.PrimitiveTypeName.INT32);
  }

  @Test
  public void shouldConvertIntegerWithLongColumnClassNameToInt64() throws SQLException {
    final Type fieldType =
        JdbcParquetSchema.buildParquetFieldType(
            "column1", Types.INTEGER, 0, "java.lang.Long", null, false, "typed_first_row");

    assertPrimitiveType(fieldType, PrimitiveType.PrimitiveTypeName.INT64);
  }

  @Test
  public void shouldConvertSmallIntSqlTypeToInt32() throws SQLException {
    final Type fieldType = buildFieldType(Types.SMALLINT, false);

    assertPrimitiveType(fieldType, PrimitiveType.PrimitiveTypeName.INT32);
  }

  @Test
  public void shouldConvertTinyIntSqlTypeToInt32() throws SQLException {
    final Type fieldType = buildFieldType(Types.TINYINT, false);

    assertPrimitiveType(fieldType, PrimitiveType.PrimitiveTypeName.INT32);
  }

  @Test
  public void shouldConvertTimestampSqlTypeToInt64() throws SQLException {
    final Type fieldType = buildFieldType(Types.TIMESTAMP, false);

    assertPrimitiveType(fieldType, PrimitiveType.PrimitiveTypeName.INT64);
    Assertions.assertNull(fieldType.asPrimitiveType().getLogicalTypeAnnotation());
  }

  @Test
  public void shouldConvertTimestampSqlTypeWithLogicalType() throws SQLException {
    final Type fieldType = buildFieldType(Types.TIMESTAMP, true);

    assertPrimitiveType(fieldType, PrimitiveType.PrimitiveTypeName.INT64);
    Assertions.assertEquals(
        LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS),
        fieldType.asPrimitiveType().getLogicalTypeAnnotation());
  }

  @Test
  public void shouldConvertDateSqlTypeToInt64() throws SQLException {
    final Type fieldType = buildFieldType(Types.DATE, false);

    assertPrimitiveType(fieldType, PrimitiveType.PrimitiveTypeName.INT64);
  }

  @Test
  public void shouldConvertDateSqlTypeWithLogicalType() throws SQLException {
    final Type fieldType = buildFieldType(Types.DATE, true);

    assertPrimitiveType(fieldType, PrimitiveType.PrimitiveTypeName.INT64);
    Assertions.assertNotNull(fieldType.asPrimitiveType().getLogicalTypeAnnotation());
  }

  @Test
  public void shouldConvertTimeSqlTypeToInt64() throws SQLException {
    final Type fieldType = buildFieldType(Types.TIME, false);

    assertPrimitiveType(fieldType, PrimitiveType.PrimitiveTypeName.INT64);
  }

  @Test
  public void shouldConvertBooleanSqlTypeToBoolean() throws SQLException {
    final Type fieldType = buildFieldType(Types.BOOLEAN, false);

    assertPrimitiveType(fieldType, PrimitiveType.PrimitiveTypeName.BOOLEAN);
  }

  @Test
  public void shouldConvertBitSqlTypeWithNoPrecisionToBoolean() throws SQLException {
    final Type fieldType = buildFieldType(Types.BIT, false);

    assertPrimitiveType(fieldType, PrimitiveType.PrimitiveTypeName.BOOLEAN);
  }

  @Test
  public void shouldConvertBitSqlTypeWithPrecision2ToBinary() throws SQLException {
    final Type fieldType =
        JdbcParquetSchema.buildParquetFieldType(
            "column1", Types.BIT, 2, "foobar", null, false, "typed_first_row");

    assertPrimitiveType(fieldType, PrimitiveType.PrimitiveTypeName.BINARY);
  }

  @Test
  public void shouldConvertBinarySqlTypeToBinary() throws SQLException {
    final Type fieldType = buildFieldType(Types.BINARY, false);

    assertPrimitiveType(fieldType, PrimitiveType.PrimitiveTypeName.BINARY);
  }

  @Test
  public void shouldConvertVarbinarySqlTypeToBinary() throws SQLException {
    final Type fieldType = buildFieldType(Types.VARBINARY, false);

    assertPrimitiveType(fieldType, PrimitiveType.PrimitiveTypeName.BINARY);
  }

  @Test
  public void shouldConvertBlobSqlTypeToBinary() throws SQLException {
    final Type fieldType = buildFieldType(Types.BLOB, false);

    assertPrimitiveType(fieldType, PrimitiveType.PrimitiveTypeName.BINARY);
  }

  @Test
  public void shouldConvertDoubleSqlTypeToDouble() throws SQLException {
    final Type fieldType = buildFieldType(Types.DOUBLE, false);

    assertPrimitiveType(fieldType, PrimitiveType.PrimitiveTypeName.DOUBLE);
  }

  @Test
  public void shouldConvertFloatSqlTypeToFloat() throws SQLException {
    final Type fieldType = buildFieldType(Types.FLOAT, false);

    assertPrimitiveType(fieldType, PrimitiveType.PrimitiveTypeName.FLOAT);
  }

  @Test
  public void shouldConvertRealSqlTypeToFloat() throws SQLException {
    final Type fieldType = buildFieldType(Types.REAL, false);

    assertPrimitiveType(fieldType, PrimitiveType.PrimitiveTypeName.FLOAT);
  }

  @Test
  public void shouldConvertVarcharSqlTypeToStringBinary() throws SQLException {
    final Type fieldType = buildFieldType(Types.VARCHAR, false);

    assertPrimitiveType(fieldType, PrimitiveType.PrimitiveTypeName.BINARY);
    Assertions.assertEquals(
        LogicalTypeAnnotation.stringType(), fieldType.asPrimitiveType().getLogicalTypeAnnotation());
  }

  @Test
  public void shouldConvertCharSqlTypeToStringBinary() throws SQLException {
    final Type fieldType = buildFieldType(Types.CHAR, false);

    assertPrimitiveType(fieldType, PrimitiveType.PrimitiveTypeName.BINARY);
    Assertions.assertEquals(
        LogicalTypeAnnotation.stringType(), fieldType.asPrimitiveType().getLogicalTypeAnnotation());
  }

  @Test
  public void shouldConvertClobSqlTypeToStringBinary() throws SQLException {
    final Type fieldType = buildFieldType(Types.CLOB, false);

    assertPrimitiveType(fieldType, PrimitiveType.PrimitiveTypeName.BINARY);
    Assertions.assertEquals(
        LogicalTypeAnnotation.stringType(), fieldType.asPrimitiveType().getLogicalTypeAnnotation());
  }

  @Test
  public void shouldConvertArraySqlTypeToListWithStringElements() throws SQLException {
    final Type fieldType =
        JdbcParquetSchema.buildParquetFieldType(
            "column1", Types.ARRAY, 0, "java.sql.Array", "_text", false, "typed_first_row");

    Assertions.assertFalse(fieldType.isPrimitive());
    Assertions.assertEquals(LogicalTypeAnnotation.listType(), fieldType.getLogicalTypeAnnotation());
    Assertions.assertEquals(Type.Repetition.OPTIONAL, fieldType.getRepetition());
  }

  @Test
  public void shouldConvertIntegerArrayToListOfInt32() {
    final Type elementType = JdbcParquetSchema.buildArrayElementType("_int4");

    assertPrimitiveType(elementType, PrimitiveType.PrimitiveTypeName.INT32);
  }

  @Test
  public void shouldConvertBigintArrayToListOfInt64() {
    final Type elementType = JdbcParquetSchema.buildArrayElementType("_int8");

    assertPrimitiveType(elementType, PrimitiveType.PrimitiveTypeName.INT64);
  }

  @Test
  public void shouldConvertFloat4ArrayToListOfFloat() {
    final Type elementType = JdbcParquetSchema.buildArrayElementType("_float4");

    assertPrimitiveType(elementType, PrimitiveType.PrimitiveTypeName.FLOAT);
  }

  @Test
  public void shouldConvertFloat8ArrayToListOfDouble() {
    final Type elementType = JdbcParquetSchema.buildArrayElementType("_float8");

    assertPrimitiveType(elementType, PrimitiveType.PrimitiveTypeName.DOUBLE);
  }

  @Test
  public void shouldConvertBoolArrayToListOfBoolean() {
    final Type elementType = JdbcParquetSchema.buildArrayElementType("_bool");

    assertPrimitiveType(elementType, PrimitiveType.PrimitiveTypeName.BOOLEAN);
  }

  @Test
  public void shouldConvertTextArrayToListOfString() {
    final Type elementType = JdbcParquetSchema.buildArrayElementType("_text");

    assertPrimitiveType(elementType, PrimitiveType.PrimitiveTypeName.BINARY);
    Assertions.assertEquals(
        LogicalTypeAnnotation.stringType(),
        elementType.asPrimitiveType().getLogicalTypeAnnotation());
  }

  @Test
  public void shouldResolveH2IntegerArrayType() {
    Assertions.assertEquals("int4", JdbcParquetSchema.resolveArrayElementTypeName("INTEGER ARRAY"));
  }

  @Test
  public void shouldResolveH2VarcharArrayType() {
    Assertions.assertEquals("text", JdbcParquetSchema.resolveArrayElementTypeName("VARCHAR ARRAY"));
  }

  @Test
  public void shouldResolveNullColumnTypeName() {
    Assertions.assertEquals("text", JdbcParquetSchema.resolveArrayElementTypeName(null));
  }

  @Test
  public void shouldConvertUuidWithLogicalType() throws SQLException {
    final Type fieldType =
        JdbcParquetSchema.buildParquetFieldType(
            "column1", Types.OTHER, 0, "foobar", "uuid", true, "typed_first_row");

    assertPrimitiveType(fieldType, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
    Assertions.assertEquals(16, fieldType.asPrimitiveType().getTypeLength());
    Assertions.assertEquals(
        LogicalTypeAnnotation.uuidType(), fieldType.asPrimitiveType().getLogicalTypeAnnotation());
  }

  @Test
  public void shouldConvertUuidWithoutLogicalType() throws SQLException {
    final Type fieldType =
        JdbcParquetSchema.buildParquetFieldType(
            "column1", Types.OTHER, 0, "foobar", "uuid", false, "typed_first_row");

    assertPrimitiveType(fieldType, PrimitiveType.PrimitiveTypeName.BINARY);
    Assertions.assertEquals(
        LogicalTypeAnnotation.stringType(), fieldType.asPrimitiveType().getLogicalTypeAnnotation());
  }

  @Test
  public void shouldDefaultConversionToStringBinary() throws SQLException {
    final Type fieldType = buildFieldType(Types.SQLXML, false);

    assertPrimitiveType(fieldType, PrimitiveType.PrimitiveTypeName.BINARY);
    Assertions.assertEquals(
        LogicalTypeAnnotation.stringType(), fieldType.asPrimitiveType().getLogicalTypeAnnotation());
  }

  @Test
  public void shouldCreateParquetSchemaFromMockResultSet() throws SQLException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(3);
    when(meta.getTableName(1)).thenReturn("test_table");
    when(meta.getTableName(2)).thenReturn("test_table");
    when(meta.getTableName(3)).thenReturn("test_table");
    when(meta.getColumnName(1)).thenReturn("id");
    when(meta.getColumnName(2)).thenReturn("name");
    when(meta.getColumnName(3)).thenReturn("active");
    when(meta.getColumnType(1)).thenReturn(Types.BIGINT);
    when(meta.getColumnType(2)).thenReturn(Types.VARCHAR);
    when(meta.getColumnType(3)).thenReturn(Types.BOOLEAN);
    when(meta.getColumnClassName(1)).thenReturn("java.lang.Long");
    when(meta.getColumnClassName(2)).thenReturn("java.lang.String");
    when(meta.getColumnClassName(3)).thenReturn("java.lang.Boolean");

    final ResultSet resultSet = Mockito.mock(ResultSet.class);
    when(resultSet.getMetaData()).thenReturn(meta);

    final MessageType schema =
        JdbcParquetSchema.createParquetSchema(
            resultSet, Optional.empty(), false, "typed_first_row");

    Assertions.assertEquals("test_table", schema.getName());
    Assertions.assertEquals(3, schema.getFieldCount());
    Assertions.assertEquals("id", schema.getFields().get(0).getName());
    Assertions.assertEquals("name", schema.getFields().get(1).getName());
    Assertions.assertEquals("active", schema.getFields().get(2).getName());
    assertPrimitiveType(schema.getFields().get(0), PrimitiveType.PrimitiveTypeName.INT64);
    assertPrimitiveType(schema.getFields().get(1), PrimitiveType.PrimitiveTypeName.BINARY);
    assertPrimitiveType(schema.getFields().get(2), PrimitiveType.PrimitiveTypeName.BOOLEAN);
    Assertions.assertTrue(schema.getFields().get(0).isRepetition(Type.Repetition.OPTIONAL));
    Assertions.assertTrue(schema.getFields().get(1).isRepetition(Type.Repetition.OPTIONAL));
    Assertions.assertTrue(schema.getFields().get(2).isRepetition(Type.Repetition.OPTIONAL));
  }

  @Test
  public void shouldUseCustomSchemaName() throws SQLException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn("test_table");
    when(meta.getColumnName(1)).thenReturn("id");
    when(meta.getColumnType(1)).thenReturn(Types.BIGINT);
    when(meta.getColumnClassName(1)).thenReturn("java.lang.Long");

    final ResultSet resultSet = Mockito.mock(ResultSet.class);
    when(resultSet.getMetaData()).thenReturn(meta);

    final MessageType schema =
        JdbcParquetSchema.createParquetSchema(
            resultSet, Optional.of("CustomName"), false, "typed_first_row");

    Assertions.assertEquals("CustomName", schema.getName());
  }

  @Test
  public void shouldNormalizeSpecialCharactersInColumnNames() throws SQLException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn("test-table.name");
    when(meta.getColumnName(1)).thenReturn("column-name.with spaces");
    when(meta.getColumnType(1)).thenReturn(Types.INTEGER);
    when(meta.getColumnClassName(1)).thenReturn("java.lang.Integer");

    final ResultSet resultSet = Mockito.mock(ResultSet.class);
    when(resultSet.getMetaData()).thenReturn(meta);

    final MessageType schema =
        JdbcParquetSchema.createParquetSchema(
            resultSet, Optional.empty(), false, "typed_first_row");

    Assertions.assertEquals("test_table_name", schema.getName());
    Assertions.assertEquals("column_name_with_spaces", schema.getFields().get(0).getName());
  }

  private Type buildFieldType(final int sqlType, final boolean useLogicalTypes) {
    return JdbcParquetSchema.buildParquetFieldType(
        "column1", sqlType, 0, "foobar", null, useLogicalTypes, "typed_first_row");
  }

  private void assertPrimitiveType(
      final Type actual, final PrimitiveType.PrimitiveTypeName expected) {
    Assertions.assertTrue(actual.isPrimitive());
    Assertions.assertEquals(expected, actual.asPrimitiveType().getPrimitiveTypeName());
  }
}
