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
import java.util.Optional;

/**
 * Wrapper class for raw SQL query (SELECT statement).
 */
public class DbeamQueryBuilder implements Serializable {

  private final StringBuilder sqlBuilder;
  private final int fromIdx;
  private Optional<String> limitStr = Optional.empty();

  private DbeamQueryBuilder(final String sqlQuery) {
    String uppSql = sqlQuery.toUpperCase();
    if (!uppSql.startsWith("SELECT")) {
      throw new IllegalArgumentException("Sql query should start with SELECT");
    }
    
    boolean isContainsWhere = false;
    if (uppSql.toUpperCase().contains("WHERE ")) {
      isContainsWhere = true;
    }

    fromIdx = uppSql.indexOf("FROM");
    if (fromIdx < 0) {
      throw new IllegalArgumentException("Sql query missing FROM clause");
    }
    
    // TODO: may be check that LIMIT is not present;
    //  can be difficult to judge in a complicated query

    this.sqlBuilder = new StringBuilder(sqlQuery);
    
    if (!isContainsWhere) {
      sqlBuilder.append(" WHERE 1=1");
    }

  }

  private DbeamQueryBuilder(DbeamQueryBuilder that) {
    this.sqlBuilder = new StringBuilder(that.sqlBuilder);
    this.fromIdx = that.fromIdx;
    this.limitStr = that.limitStr;
  }

  public DbeamQueryBuilder copy() {
    return new DbeamQueryBuilder(this);
  }

  public static DbeamQueryBuilder fromTablename(final String tableName) {
    return new DbeamQueryBuilder(String.format("SELECT * FROM %s WHERE 1=1", tableName));
  }

  public static DbeamQueryBuilder fromSqlQuery(final String sqlQuery) {
    String s = removeTrailingSymbols(sqlQuery);
    return new DbeamQueryBuilder(s);
  }

  public DbeamQueryBuilder withPartitionCondition(
          String partitionColumn, String startPointIncl, String endPointExcl) {
    sqlBuilder.append(createSqlPartitionCondition(partitionColumn, startPointIncl, endPointExcl));
    return this;
  }
          
  // TODO It is assumed now that partitionColumn is of non-numeric type 
  private static String createSqlPartitionCondition(
      String partitionColumn, String startPointIncl, String endPointExcl) {
    return String.format(
        " AND %s >= '%s' AND %s < '%s'",
        partitionColumn, startPointIncl, partitionColumn, endPointExcl);
  }

  public DbeamQueryBuilder withParallelizationCondition(
      String partitionColumn, long startPointIncl, long endPoint, boolean isEndPointExcl) {
    sqlBuilder.append(
        createSqlSplitCondition(partitionColumn, startPointIncl, endPoint, isEndPointExcl));
    return this;
  }

  private static String createSqlSplitCondition(
          String partitionColumn, long startPointIncl, long endPoint, boolean isEndPointExcl) {
    
    String upperBoundOperation = isEndPointExcl ? "<" : "<=";
    return String.format(
            " AND %s >= %s AND %s %s %s",
            partitionColumn, startPointIncl, partitionColumn, upperBoundOperation, endPoint);
  }

  /**
   * Returns generated SQL query string.
   * 
   * @return generated SQL query string.
   */
  public String build() {
    limitStr.map(x -> sqlBuilder.append(x));
    limitStr = Optional.empty();
    return sqlBuilder.toString();
  }

  private static String removeTrailingSymbols(String sqlQuery) {
    return sqlQuery.replaceAll("[\\s|;]+$", "");
  }

  public DbeamQueryBuilder withLimit(Optional<Integer> limitOpt) {
    return limitOpt.map(l -> this.withLimit(l)).orElse(this);
  }
  
  public DbeamQueryBuilder withLimit(long limit) {
    limitStr = Optional.of(String.format(" LIMIT %d", limit));
    return this;
  }

  public DbeamQueryBuilder withLimitOne() {
    return withLimit(1L);
  }

  @Override
  public String toString() {
    return build();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof DbeamQueryBuilder) {
      DbeamQueryBuilder that = (DbeamQueryBuilder) obj;
      return build().equals((that.build())) && limitStr.equals(that.limitStr);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return sqlBuilder.hashCode();
  }

  /**
   * Generates a new query to get MIN/MAX values for splitColumn.  
   * 
   * @param splitColumn column to use
   * @param minSplitColumnName MIN() column value alias
   * @param maxSplitColumnName MAX() column value alias
   * @return a new query builder
   */
  public DbeamQueryBuilder generateQueryToGetLimitsOfSplitColumn(
      String splitColumn,
      String minSplitColumnName,
      String maxSplitColumnName) {

    return DbeamQueryBuilder.fromSqlQuery(
        String.format(
            "SELECT MIN(%s) as %s, MAX(%s) as %s %s",
            splitColumn,
            minSplitColumnName,
            splitColumn,
            maxSplitColumnName,
            sqlBuilder.substring(fromIdx)));
  }

}
