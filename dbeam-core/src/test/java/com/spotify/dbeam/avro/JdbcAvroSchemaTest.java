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

import static com.spotify.dbeam.avro.JdbcAvroSchema.getSqlTypeName;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Optional;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class JdbcAvroSchemaTest {

  /** Stores type mapping SQL type => Avro type. */
  private static class TypeMapping {
    private final int sqlType;
    private final Schema.Type avroType;

    public TypeMapping(int sqlType, Schema.Type avroType) {
      this.sqlType = sqlType;
      this.avroType = avroType;
    }
  }

  private static final TypeMapping[] allSqlTypes =
      new TypeMapping[] {
        new TypeMapping(java.sql.Types.BIT, Schema.Type.BOOLEAN),
        new TypeMapping(java.sql.Types.TINYINT, Schema.Type.INT),
        new TypeMapping(java.sql.Types.SMALLINT, Schema.Type.INT),
        new TypeMapping(java.sql.Types.INTEGER, Schema.Type.INT),
        new TypeMapping(java.sql.Types.BIGINT, Schema.Type.LONG),
        new TypeMapping(java.sql.Types.FLOAT, Schema.Type.FLOAT),
        new TypeMapping(java.sql.Types.REAL, Schema.Type.FLOAT),
        new TypeMapping(java.sql.Types.DOUBLE, Schema.Type.DOUBLE),
        new TypeMapping(java.sql.Types.NUMERIC, Schema.Type.STRING), // default TODO ?
        new TypeMapping(java.sql.Types.DECIMAL, Schema.Type.STRING), // default TODO ?
        new TypeMapping(java.sql.Types.CHAR, Schema.Type.STRING),
        new TypeMapping(java.sql.Types.VARCHAR, Schema.Type.STRING),
        new TypeMapping(java.sql.Types.LONGVARCHAR, Schema.Type.STRING),
        new TypeMapping(java.sql.Types.DATE, Schema.Type.LONG),
        new TypeMapping(java.sql.Types.TIME, Schema.Type.LONG),
        new TypeMapping(java.sql.Types.TIMESTAMP, Schema.Type.LONG),
        new TypeMapping(java.sql.Types.BINARY, Schema.Type.BYTES),
        new TypeMapping(java.sql.Types.VARBINARY, Schema.Type.BYTES),
        new TypeMapping(java.sql.Types.LONGVARBINARY, Schema.Type.BYTES),
        new TypeMapping(java.sql.Types.NULL, Schema.Type.STRING), // default
        new TypeMapping(java.sql.Types.OTHER, Schema.Type.STRING), // default
        new TypeMapping(java.sql.Types.JAVA_OBJECT, Schema.Type.STRING), // default
        new TypeMapping(java.sql.Types.DISTINCT, Schema.Type.STRING), // default
        new TypeMapping(java.sql.Types.STRUCT, Schema.Type.STRING), // default
        new TypeMapping(java.sql.Types.ARRAY, Schema.Type.BYTES),
        new TypeMapping(java.sql.Types.BLOB, Schema.Type.BYTES),
        new TypeMapping(java.sql.Types.CLOB, Schema.Type.STRING),
        new TypeMapping(java.sql.Types.REF, Schema.Type.STRING), // default
        new TypeMapping(java.sql.Types.DATALINK, Schema.Type.STRING), // default
        new TypeMapping(java.sql.Types.BOOLEAN, Schema.Type.BOOLEAN),
        new TypeMapping(java.sql.Types.ROWID, Schema.Type.STRING), // default
        new TypeMapping(java.sql.Types.NCHAR, Schema.Type.STRING),
        new TypeMapping(java.sql.Types.NVARCHAR, Schema.Type.STRING), // default TODO ?
        new TypeMapping(java.sql.Types.LONGNVARCHAR, Schema.Type.STRING),
        new TypeMapping(java.sql.Types.NCLOB, Schema.Type.STRING), // default TODO ?
        new TypeMapping(java.sql.Types.SQLXML, Schema.Type.STRING), // default TODO ?
        new TypeMapping(java.sql.Types.REF_CURSOR, Schema.Type.STRING), // default ?
        new TypeMapping(java.sql.Types.TIME_WITH_TIMEZONE, Schema.Type.LONG),
        new TypeMapping(
            java.sql.Types.TIMESTAMP_WITH_TIMEZONE, Schema.Type.STRING) // default TODO ?
      };

  public static final int COLUMN_NUM = 1;

  @Test
  public void shouldGetDatabaseTableNameFromMetaData() throws SQLException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn("test_table");

    Assert.assertEquals("test_table", JdbcAvroSchema.getDatabaseTableName(meta));
  }

  @Test
  public void shouldDefaultTableNameWhenMetaDataHasEmptyTableName() throws SQLException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn("");

    Assert.assertEquals("no_table_name", JdbcAvroSchema.getDatabaseTableName(meta));
  }

  @Test
  public void shouldDefaultTableNameWhenMetaDataHasNullTableName() throws SQLException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn(null);

    Assert.assertEquals("no_table_name", JdbcAvroSchema.getDatabaseTableName(meta));
  }

  @Test
  public void shouldGetDatabaseTableNameFromFirstNonNullMetaData() throws SQLException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(2);
    when(meta.getTableName(1)).thenReturn("");
    when(meta.getTableName(2)).thenReturn("test_table");

    Assert.assertEquals("test_table", JdbcAvroSchema.getDatabaseTableName(meta));
  }

  @Test
  public void shouldConvertDateSqlTypeWithAvroLogicalType() throws SQLException {
    final ResultSet resultSet = buildMockResultSet(Types.DATE);

    final Schema fieldSchema = createAvroSchemaForSingleField(resultSet, true);

    Assert.assertEquals(Schema.Type.LONG, fieldSchema.getType());
    Assert.assertEquals("timestamp-millis", fieldSchema.getProp("logicalType"));
  }

  @Test
  public void shouldNotConvertNotNullSqlTypeToAvroNotNullType() throws SQLException {
    final ResultSet resultSet = buildMockResultSetWithNotNull(Types.NCHAR, false);

    final Schema avroSchema = createAvroSchema(resultSet, true);
    final Schema.Field field = avroSchema.getField("column1");

    Assert.assertEquals(Schema.Type.UNION, field.schema().getType());
    Assert.assertEquals(
        Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)),
        field.schema().getTypes());
    Assert.assertTrue(field.hasDefaultValue()); // null represented as NullNode()
  }

  @Test
  public void shouldConvertNotNullSqlTypeToAvroNotNullType() throws SQLException {
    for (int i = 0; i < allSqlTypes.length; i++) {
      TypeMapping typeMapping = allSqlTypes[i];
      int sqlType = typeMapping.sqlType;
      Schema.Type expectedAvroType = typeMapping.avroType;

      final ResultSet resultSet = buildMockResultSetWithNotNull(sqlType, true);

      final boolean useNotNullTypes = true;
      final Schema avroSchema = createAvroSchema(resultSet, false, useNotNullTypes);
      final Schema.Field field = avroSchema.getField("column1");

      String message =
          String.format(
              "Mapping #[%d] SQL [%d][%s] => Avro [%s] failed",
              i, sqlType, getSqlTypeName(sqlType), expectedAvroType);
      Assert.assertEquals(message, expectedAvroType, field.schema().getType());
      Assert.assertFalse(field.hasDefaultValue()); // no default value set
    }
  }

  @Test
  public void shouldConvertDateSqlTypeWithoutAvroLogicalType() throws SQLException {
    final ResultSet resultSet = buildMockResultSet(Types.DATE);

    final Schema fieldSchema = createAvroSchemaForSingleField(resultSet, false);

    Assert.assertEquals(Schema.Type.LONG, fieldSchema.getType());
    Assert.assertNull(fieldSchema.getProp("logicalType"));
  }

  @Test
  public void shouldConvertBigIntSqlTypeToLong() throws SQLException {
    final ResultSet resultSet = buildMockResultSet(Types.BIGINT);

    final Schema fieldSchema = createAvroSchemaForSingleField(resultSet, false);

    Assert.assertEquals(Schema.Type.LONG, fieldSchema.getType());
  }

  @Test
  public void shouldConvertBitSqlTypeWithNoPrecisionToBoolean() throws SQLException {
    final ResultSet resultSet = buildMockResultSet(Types.BIT);

    final Schema fieldSchema = createAvroSchemaForSingleField(resultSet, false);

    Assert.assertEquals(Schema.Type.BOOLEAN, fieldSchema.getType());
  }

  @Test
  public void shouldConvertBitSqlTypeWithPrecision2ToBytes() throws SQLException {
    final ResultSet resultSet = buildMockResultSet(Types.BIT);
    when(resultSet.getMetaData().getPrecision(COLUMN_NUM)).thenReturn(2);

    final Schema fieldSchema = createAvroSchemaForSingleField(resultSet, false);

    Assert.assertEquals(Schema.Type.BYTES, fieldSchema.getType());
  }

  @Test
  public void shouldConvertIntegerWithLongColumnClassNameToLong() throws SQLException {
    final ResultSet resultSet = buildMockResultSet(Types.INTEGER);
    when(resultSet.getMetaData().getColumnClassName(COLUMN_NUM)).thenReturn("java.lang.Long");

    final Schema fieldSchema = createAvroSchemaForSingleField(resultSet, false);

    Assert.assertEquals(Schema.Type.LONG, fieldSchema.getType());
  }

  @Test
  public void shouldConvertIntegerSqlTypeToInteger() throws SQLException {
    final ResultSet resultSet = buildMockResultSet(Types.INTEGER);

    final Schema fieldSchema = createAvroSchemaForSingleField(resultSet, false);

    Assert.assertEquals(Schema.Type.INT, fieldSchema.getType());
  }

  @Test
  public void shouldDefaultConversionToStringType() throws SQLException {
    final ResultSet resultSet = buildMockResultSet(Types.SQLXML);

    final Schema fieldSchema = createAvroSchemaForSingleField(resultSet, false);

    Assert.assertEquals(Schema.Type.STRING, fieldSchema.getType());
  }

  private Schema createAvroSchema(final ResultSet resultSet, final boolean useLogicalTypes)
      throws SQLException {
    return createAvroSchema(resultSet, useLogicalTypes, false);
  }

  private Schema createAvroSchema(
      final ResultSet resultSet, final boolean useLogicalTypes, final boolean useNotNullTypes)
      throws SQLException {
    Schema avroSchema =
        JdbcAvroSchema.createAvroSchema(
            resultSet,
            "namespace1",
            "url1",
            Optional.empty(),
            "doc1",
            useLogicalTypes,
            useNotNullTypes);

    return avroSchema;
  }

  private Schema createAvroSchemaForSingleField(
      final ResultSet resultSet, final boolean useLogicalTypes) throws SQLException {
    Schema avroSchema = createAvroSchema(resultSet, useLogicalTypes);

    return avroSchema.getField("column1").schema().getTypes().get(COLUMN_NUM);
  }

  private ResultSet buildMockResultSet(final int inputColumnType) throws SQLException {
    return buildMockResultSetWithNotNull(inputColumnType, false);
  }

  private ResultSet buildMockResultSetWithNotNull(
      final int inputColumnType, final boolean isNotNull) throws SQLException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(COLUMN_NUM);
    when(meta.getTableName(COLUMN_NUM)).thenReturn("test_table");
    when(meta.getColumnName(COLUMN_NUM)).thenReturn("column1");
    when(meta.getColumnType(COLUMN_NUM)).thenReturn(inputColumnType);
    when(meta.getColumnClassName(COLUMN_NUM)).thenReturn("foobar");
    final int isNullableType =
        isNotNull ? ResultSetMetaData.columnNoNulls : ResultSetMetaData.columnNullable;
    when(meta.isNullable(COLUMN_NUM)).thenReturn(isNullableType);

    final ResultSet resultSet = Mockito.mock(ResultSet.class);
    when(resultSet.getMetaData()).thenReturn(meta);
    return resultSet;
  }
}
