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

package com.spotify.dbeam.args;

import java.io.Serializable;

/**
 * Wrapper class for raw SQL query (SELECT statement).
 */
public class SqlQueryWrapper implements Serializable {

  private final String sqlQuery;

  private SqlQueryWrapper(final String sqlQuery) {

    this.sqlQuery = sqlQuery;
  }
  
  // TODO: move to a constructor ?
  public static Boolean checkSqlQuery(String sqlQuery) {
    String uppSql = sqlQuery.toUpperCase();
    // some 'light' checks applied
    // TODO: may be check that LIMIT is not present
    return uppSql.contains("SELECT") && uppSql.contains("FROM");
  }

  public String getSqlQuery() {
    return sqlQuery;
  }

  public static SqlQueryWrapper ofRawSql(final String sqlQuery) {
    if (sqlQuery.toUpperCase().contains("WHERE ")) {
      return new SqlQueryWrapper(sqlQuery);
    } else {
      return new SqlQueryWrapper(String.format("%s WHERE 1=1", sqlQuery));
    }
  }

  public static SqlQueryWrapper ofTablename(final String tableName) {
    return new SqlQueryWrapper(String.format("SELECT * FROM %s WHERE 1=1", tableName));
  }

  public String addLimit() {
    return String.format("%s LIMIT 1", sqlQuery);
  }

  @Override
  public String toString() {
    return sqlQuery;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof SqlQueryWrapper) {
      return getSqlQuery().equals((((SqlQueryWrapper) obj).getSqlQuery()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    return sqlQuery.hashCode();
  }

  public SqlQueryWrapper generateQueryToGetLimitsOfSplitColumn(
      String partitionCondition,
      String splitColumn,
      String minSplitColumnName,
      String maxSplitColumnName) {
    int fromIdx = sqlQuery.toUpperCase().indexOf("FROM");
    // cannot return -1, we have already checked/ensured that.

    return SqlQueryWrapper.ofRawSql(
        String.format(
            "SELECT min(%s) as min_s, max(%s) as max_s %s%s",
            splitColumn, splitColumn, sqlQuery.substring(fromIdx), partitionCondition));
  }
}
