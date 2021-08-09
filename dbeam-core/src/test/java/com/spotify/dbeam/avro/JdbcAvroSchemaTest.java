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
    ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn("test_table");
    when(meta.getColumnName(1)).thenReturn("datex");
    when(meta.getColumnType(1)).thenReturn(java.sql.Types.DATE);

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

    Schema.Type expected = Schema.Type.LONG;
    Schema.Field datex = avroSchema.getField("datex");
    Assert.assertEquals(expected, datex.schema().getTypes().get(1).getType());
    Assert.assertEquals(
        "timestamp-millis", datex.schema().getTypes().get(1).getProp("logicalType"));
  }
}
