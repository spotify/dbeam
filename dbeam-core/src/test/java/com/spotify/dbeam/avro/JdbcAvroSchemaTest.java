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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
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
  public void checkTableNameForCastAndOrdinaryColumns() throws SQLException {
    ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(2);
    when(meta.getTableName(1)).thenReturn("");
    when(meta.getTableName(2)).thenReturn("test_table");

    String expected = "test_table";
    String tableName = JdbcAvroSchema.getDatabaseTableName(meta);

    Assert.assertEquals(expected, tableName);
  }
}
