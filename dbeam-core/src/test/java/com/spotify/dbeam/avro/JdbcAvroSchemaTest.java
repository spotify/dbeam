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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Optional;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class JdbcAvroSchemaTest {

  @Test
  public void checkTableNameForOrdinaryColumn() throws SQLException {
    ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn("test_table");

    String expected = "test_table";
    String tableName = JdbcAvroSchema.getDatabaseTableName(meta);

    Assert.assertEquals(expected, tableName);
  }

  @Test
  public void checkTableNameForCastColumn() throws SQLException {
    ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn("");

    String expected = "no_table_name";
    String tableName = JdbcAvroSchema.getDatabaseTableName(meta);

    Assert.assertEquals(expected, tableName);
  }

  @Test
  public void checkTableNameForCastColumnNull() throws SQLException {
    ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn(null);

    String expected = "no_table_name";
    String tableName = JdbcAvroSchema.getDatabaseTableName(meta);

    Assert.assertEquals(expected, tableName);
  }

  @Test
  public void checkTableNameForCastAndOrdinaryColumns() throws SQLException {
    ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(2);
    when(meta.getTableName(1)).thenReturn("");
    when(meta.getTableName(2)).thenReturn("test_table");

    String expected = "test_table";
    String tableName = JdbcAvroSchema.getDatabaseTableName(meta);

    Assert.assertEquals(expected, tableName);
  }

  @Test
  public void checkAvroTypeForSqlDateWithLogicalType() throws SQLException {

    final int inputType = Types.DATE;
    final Schema.Type outputType = Schema.Type.LONG;
    final String expectedLogicalType = "timestamp-millis";
    final int fieldPrecision = 0;

    verifyInputVsOutputType(inputType, outputType, expectedLogicalType, fieldPrecision);
  }

  @Test
  public void checkAvroTypeForSqlBigintWithPrecisionZero() throws SQLException {

    final int inputType = Types.BIGINT;
    final Schema.Type outputType = Schema.Type.STRING;
    final String expectedLogicalType = null;
    final int fieldPrecision = 0;

    verifyInputVsOutputType(inputType, outputType, expectedLogicalType, fieldPrecision);
  }

  @Test
  public void checkAvroTypeForSqlBitWithPrecisionOne() throws SQLException {

    final int inputType = Types.BIT;
    final Schema.Type outputType = Schema.Type.BOOLEAN;
    final String expectedLogicalType = null;
    final int fieldPrecision = 1;

    verifyInputVsOutputType(inputType, outputType, expectedLogicalType, fieldPrecision);
  }

  @Test
  public void checkAvroTypeForSqlBitWithPrecisionTwo() throws SQLException {

    final int inputType = Types.BIT;
    final Schema.Type outputType = Schema.Type.BYTES;
    final String expectedLogicalType = null;
    final int fieldPrecision = 2;

    verifyInputVsOutputType(inputType, outputType, expectedLogicalType, fieldPrecision);
  }

  private void verifyInputVsOutputType(
      int inputColumnType,
      Schema.Type outputColumnType,
      String expectedLogicalType,
      int fieldPrecision)
      throws SQLException {
    ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    int columnNum = 1;
    when(meta.getColumnCount()).thenReturn(columnNum);
    when(meta.getTableName(columnNum)).thenReturn("test_table");
    when(meta.getColumnName(columnNum)).thenReturn("datex");
    when(meta.getColumnType(columnNum)).thenReturn(inputColumnType);
    when(meta.getColumnClassName(columnNum)).thenReturn("java.lang.Long"); // the same value for all
    when(meta.getPrecision(columnNum)).thenReturn(fieldPrecision);

    final ResultSet resultSet = Mockito.mock(ResultSet.class);
    when(resultSet.getMetaData()).thenReturn(meta);

    final String avroSchemaNamespace = "namespace1";
    final String connectionUrl = "url1";
    final Optional<String> maybeSchemaName = Optional.empty();
    final String avroDoc = "doc1";
    final boolean useLogicalTypes = true;
    Schema avroSchema =
        JdbcAvroSchema.createAvroSchema(
            resultSet,
            avroSchemaNamespace,
            connectionUrl,
            maybeSchemaName,
            avroDoc,
            useLogicalTypes);

    Schema.Field datex = avroSchema.getField("datex");
    Assert.assertEquals(outputColumnType, datex.schema().getTypes().get(columnNum).getType());

    if (expectedLogicalType != null) {
      Assert.assertEquals(
          expectedLogicalType, datex.schema().getTypes().get(columnNum).getProp("logicalType"));
    }
  }

  @Test
  public void verifySqlTypeUnsignedIntConvertedToAvroLong() throws SQLException {

    final int inputType = java.sql.Types.INTEGER;
    final Schema.Type outputType = Schema.Type.LONG;

    final String expectedLogicalType = null;
    final int fieldPrecision = 10;

    verifyInputVsOutputType(inputType, outputType, expectedLogicalType, fieldPrecision);
  }
}
