/*-
 * -\-\-
 * DBeam Core
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
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

import com.spotify.dbeam.options.ArrayHandlingMode;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Optional;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class JdbcAvroSchemaTest {

  public static final int COLUMN_NUM = 1;

  @Test
  public void shouldGetDatabaseTableNameFromMetaData() throws SQLException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn("test_table");

    Assertions.assertEquals("test_table", JdbcAvroSchema.getDatabaseTableName(meta));
  }

  @Test
  public void shouldDefaultTableNameWhenMetaDataHasEmptyTableName() throws SQLException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn("");

    Assertions.assertEquals("no_table_name", JdbcAvroSchema.getDatabaseTableName(meta));
  }

  @Test
  public void shouldDefaultTableNameWhenMetaDataHasNullTableName() throws SQLException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn(null);

    Assertions.assertEquals("no_table_name", JdbcAvroSchema.getDatabaseTableName(meta));
  }

  @Test
  public void shouldGetDatabaseTableNameFromFirstNonNullMetaData() throws SQLException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(2);
    when(meta.getTableName(1)).thenReturn("");
    when(meta.getTableName(2)).thenReturn("test_table");

    Assertions.assertEquals("test_table", JdbcAvroSchema.getDatabaseTableName(meta));
  }

  @Test
  public void shouldConvertDateSqlTypeWithAvroLogicalType() throws SQLException {
    final ResultSet resultSet = buildMockResultSet(Types.DATE);

    final Schema fieldSchema = createAvroSchemaForSingleField(resultSet, true);

    Assertions.assertEquals(Schema.Type.LONG, fieldSchema.getType());
    Assertions.assertEquals("timestamp-millis", fieldSchema.getProp("logicalType"));
  }

  @Test
  public void shouldConvertDateSqlTypeWithoutAvroLogicalType() throws SQLException {
    final ResultSet resultSet = buildMockResultSet(Types.DATE);

    final Schema fieldSchema = createAvroSchemaForSingleField(resultSet, false);

    Assertions.assertEquals(Schema.Type.LONG, fieldSchema.getType());
    Assertions.assertNull(fieldSchema.getProp("logicalType"));
  }

  @Test
  public void shouldConvertBigIntSqlTypeToLong() throws SQLException {
    final ResultSet resultSet = buildMockResultSet(Types.BIGINT);

    final Schema fieldSchema = createAvroSchemaForSingleField(resultSet, false);

    Assertions.assertEquals(Schema.Type.LONG, fieldSchema.getType());
  }

  @Test
  public void shouldConvertBitSqlTypeWithNoPrecisionToBoolean() throws SQLException {
    final ResultSet resultSet = buildMockResultSet(Types.BIT);

    final Schema fieldSchema = createAvroSchemaForSingleField(resultSet, false);

    Assertions.assertEquals(Schema.Type.BOOLEAN, fieldSchema.getType());
  }

  @Test
  public void shouldConvertBitSqlTypeWithPrecision2ToBytes() throws SQLException {
    final ResultSet resultSet = buildMockResultSet(Types.BIT);
    when(resultSet.getMetaData().getPrecision(COLUMN_NUM)).thenReturn(2);

    final Schema fieldSchema = createAvroSchemaForSingleField(resultSet, false);

    Assertions.assertEquals(Schema.Type.BYTES, fieldSchema.getType());
  }

  @Test
  public void shouldThrowOnNonSupportedTypes() throws SQLException {
    final ResultSet resultSet = buildMockResultSet(Types.STRUCT);
    RuntimeException thrown =
        Assertions.assertThrows(
            RuntimeException.class, () -> createAvroSchemaForSingleField(resultSet, false));
    Assertions.assertEquals("STRUCT type is not supported", thrown.getMessage());

    final ResultSet resultSet2 = buildMockResultSet(Types.REF);
    RuntimeException thrown2 =
        Assertions.assertThrows(
            RuntimeException.class, () -> createAvroSchemaForSingleField(resultSet2, false));
    Assertions.assertEquals("REF and REF_CURSOR type are not supported", thrown2.getMessage());

    final ResultSet resultSet3 = buildMockResultSet(Types.REF_CURSOR);
    RuntimeException thrown3 =
        Assertions.assertThrows(
            RuntimeException.class, () -> createAvroSchemaForSingleField(resultSet3, false));
    Assertions.assertEquals("REF and REF_CURSOR type are not supported", thrown3.getMessage());

    final ResultSet resultSet4 = buildMockResultSet(Types.DATALINK);
    RuntimeException thrown4 =
        Assertions.assertThrows(
            RuntimeException.class, () -> createAvroSchemaForSingleField(resultSet4, false));
    Assertions.assertEquals("DATALINK type is not supported", thrown4.getMessage());
  }

  @Test
  public void shouldConvertIntegerWithLongColumnClassNameToLong() throws SQLException {
    final ResultSet resultSet = buildMockResultSet(Types.INTEGER);
    when(resultSet.getMetaData().getColumnClassName(COLUMN_NUM)).thenReturn("java.lang.Long");

    final Schema fieldSchema = createAvroSchemaForSingleField(resultSet, false);

    Assertions.assertEquals(Schema.Type.LONG, fieldSchema.getType());
  }

  @Test
  public void shouldConvertIntegerSqlTypeToInteger() throws SQLException {
    final ResultSet resultSet = buildMockResultSet(Types.INTEGER);

    final Schema fieldSchema = createAvroSchemaForSingleField(resultSet, false);

    Assertions.assertEquals(Schema.Type.INT, fieldSchema.getType());
  }

  @Test
  public void shouldDefaultConversionToStringType() throws SQLException {
    final ResultSet resultSet = buildMockResultSet(Types.SQLXML);

    final Schema fieldSchema = createAvroSchemaForSingleField(resultSet, false);

    Assertions.assertEquals(Schema.Type.STRING, fieldSchema.getType());
  }

  @Test
  public void shouldConvertUuidSqlTypeWithAvroLogicalType() throws SQLException {
    final ResultSet resultSet = buildMockResultSet(Types.OTHER, "uuid");
    final Schema fieldSchema = createAvroSchemaForSingleField(resultSet, true);

    Assertions.assertEquals(Schema.Type.STRING, fieldSchema.getType());
    Assertions.assertEquals("uuid", fieldSchema.getProp("logicalType"));
  }

  @Test
  public void shouldConvertUuidSqlTypeWithoutAvroLogicalType() throws SQLException {
    final ResultSet resultSet = buildMockResultSet(Types.OTHER, "uuid");
    final Schema fieldSchema = createAvroSchemaForSingleField(resultSet, false);

    Assertions.assertEquals(Schema.Type.STRING, fieldSchema.getType());
    Assertions.assertNull(fieldSchema.getProp("logicalType"));
  }

  private Schema createAvroSchemaForSingleField(
      final ResultSet resultSet, final boolean useLogicalTypes) throws SQLException {
    return createAvroSchemaForSingleField(resultSet, useLogicalTypes, false);
  }

  private Schema createAvroSchemaForSingleField(
      final ResultSet resultSet, final boolean useLogicalTypes,
      final boolean useTimestampMicros) throws SQLException {
    Schema avroSchema =
        JdbcAvroSchema.createAvroSchema(
            resultSet, "namespace1", "url1", Optional.empty(), "doc1", useLogicalTypes,
            useTimestampMicros, ArrayHandlingMode.TypedMetaFromFirstRow, false);

    return avroSchema.getField("column1").schema().getTypes().get(COLUMN_NUM);
  }

  private ResultSet buildMockResultSet(final int inputColumnType) throws SQLException {
    return buildMockResultSet(inputColumnType, null);
  }

  private ResultSet buildMockResultSet(final int inputColumnType,
                                       final String columnTypeName) throws SQLException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(COLUMN_NUM);
    when(meta.getTableName(COLUMN_NUM)).thenReturn("test_table");
    when(meta.getColumnName(COLUMN_NUM)).thenReturn("column1");
    when(meta.getColumnType(COLUMN_NUM)).thenReturn(inputColumnType);
    when(meta.getColumnClassName(COLUMN_NUM)).thenReturn("foobar");
    if (columnTypeName != null) {
      when(meta.getColumnTypeName(COLUMN_NUM)).thenReturn(columnTypeName);
    }

    final ResultSet resultSet = Mockito.mock(ResultSet.class);
    when(resultSet.getMetaData()).thenReturn(meta);
    return resultSet;
  }
}
