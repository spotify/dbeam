/*-
 * -\-\-
 * DBeam Core
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
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

package com.spotify.dbeam;

import static org.mockito.Mockito.when;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import org.mockito.Mockito;

public class JavaSqlHelper {

  public static class DummyResultSetMetaData {

    public static ResultSetMetaData create(int columnCount) throws SQLException {

      String[] tableNames = new String[columnCount];
      String[] columnNames = new String[columnCount];
      int[] columnTypes = new int[columnCount];
      for (int i = 0; i < columnCount; i++) {
        tableNames[i] = "dummyTableName" + i;
        columnNames[i] = "dummyColumnName" + i;
        columnTypes[i] = Types.CHAR;
      }
      return create(columnCount, tableNames, columnNames, columnTypes);
    }

    public static ResultSetMetaData create(
        int columnCount, String[] tableNames, String[] columnNames, int[] columnTypes)
        throws SQLException {
      ResultSetMetaData rsMetaData = Mockito.mock(ResultSetMetaData.class);
      when(rsMetaData.getColumnCount()).thenReturn(columnCount);
      for (int i = 0; i < columnCount; i++) {
        when(rsMetaData.getTableName(i + 1)).thenReturn(tableNames[i]);
        when(rsMetaData.getColumnName(i + 1)).thenReturn(columnNames[i]);
        when(rsMetaData.getColumnType(i + 1)).thenReturn(columnTypes[i]);
        when(rsMetaData.getColumnClassName(i + 1)).thenReturn("dummy");
      }
      return rsMetaData;
    }
  }
}
