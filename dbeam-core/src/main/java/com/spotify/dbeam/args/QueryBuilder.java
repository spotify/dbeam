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
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * Wrapper class for raw SQL query.
 */
public class QueryBuilder implements Serializable {

  //private static final char SQL_STATEMENT_TERMINATOR = ';';
  private static final String DEFAULT_SELECT_CLAUSE = "SELECT *";
  private static final String DEFAULT_WHERE_CLAUSE = "WHERE 1=1";


  interface QueryBase {

    String getBaseSql();

    QueryBase copyWithSelect(final String selectClause);
  }

  /**
   * Represents table-based query, which we have full control of.
   * 
   * <p>Immutable entity. 
   */
  private static class TableQueryBase implements QueryBase {

    private final String tableName;
    private final String selectClause;

    public TableQueryBase(final String tableName) {
      this(tableName, DEFAULT_SELECT_CLAUSE);
    }

    public TableQueryBase(final String tableName, final String selectClause) {
      this.tableName = tableName;
      this.selectClause = selectClause;
    }

    @Override
    public String getBaseSql() {
      return String.format("%s FROM %s %s",
              selectClause, tableName, DEFAULT_WHERE_CLAUSE);
    }

    @Override
    public TableQueryBase copyWithSelect(final String selectClause) {
      return new TableQueryBase(this.tableName, selectClause);
    }

    @Override
    public int hashCode() {
      return tableName.hashCode();
    }
  }

  /**
   * Represents user-provided raw query, which we have no control of.
   *
   * <p>Immutable entity. 
   */
  private static class UserQueryBase implements QueryBase {

    private final String userSqlQuery;
    private final String selectClause;

    public UserQueryBase(final String userSqlQuery) {
      this(userSqlQuery, DEFAULT_SELECT_CLAUSE);
    }

    public UserQueryBase(final String userSqlQuery, final String selectClause) {
      this.userSqlQuery = removeTrailingSymbols(userSqlQuery);
      this.selectClause = selectClause;
    }
    
    @Override
    public String getBaseSql() {
      return String.format("%s FROM (%s) %s",
              selectClause, userSqlQuery, DEFAULT_WHERE_CLAUSE);
    }

    @Override
    public UserQueryBase copyWithSelect(String selectClause) {
      return new UserQueryBase(this.userSqlQuery, selectClause);
    }

    @Override
    public int hashCode() {
      return userSqlQuery.hashCode();
    }
  }

  private final QueryBase base;
  private final List<String> whereConditions = new LinkedList<>();
  private Optional<String> limitStr = Optional.empty();
  
  private QueryBuilder(final QueryBase base) {
    this.base = base;
  }

  private QueryBuilder(final QueryBase base, final QueryBuilder that) {
    this.base = base;
    this.whereConditions.addAll(that.whereConditions);
    this.limitStr = that.limitStr;
  }

  private QueryBuilder(final QueryBuilder that) {
    this.base = that.base;
    this.whereConditions.addAll(that.whereConditions);
    this.limitStr = that.limitStr;
  }

  public QueryBuilder copy() {
    return new QueryBuilder(this);
  }

  public static QueryBuilder fromTablename(final String tableName) {
    return new QueryBuilder(new TableQueryBase(tableName));
  }

  public static QueryBuilder fromSqlQuery(final String sqlQuery) {
    return new QueryBuilder(new UserQueryBase(sqlQuery));
  }

  public QueryBuilder withPartitionCondition(
          String partitionColumn, String startPointIncl, String endPointExcl) {
    whereConditions.add(createSqlPartitionCondition(partitionColumn, startPointIncl, endPointExcl));
    return this;
  }
          
  private static String createSqlPartitionCondition(
      String partitionColumn, String startPointIncl, String endPointExcl) {
    return String.format(
        " AND %s >= '%s' AND %s < '%s'",
        partitionColumn, startPointIncl, partitionColumn, endPointExcl);
  }

  public QueryBuilder withParallelizationCondition(
      String partitionColumn, long startPointIncl, long endPoint, boolean isEndPointExcl) {
    whereConditions.add(
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
    String initial = base.getBaseSql();
    StringBuilder buffer = new StringBuilder(initial);
    whereConditions.forEach(x -> buffer.append(x));
    limitStr.ifPresent(x -> buffer.append(x));
    //buffer.append(SQL_STATEMENT_TERMINATOR);
    return buffer.toString();
  }

  private static String removeTrailingSymbols(String sqlQuery) {
    return sqlQuery.replaceAll("[\\s|;]+$", "");
  }

  public QueryBuilder withLimit(Optional<Long> limitOpt) {
    return limitOpt.map(l -> this.withLimit(l)).orElse(this);
  }
  
  public QueryBuilder withLimit(long limit) {
    limitStr = Optional.of(String.format(" LIMIT %d", limit));
    return this;
  }

  public QueryBuilder withLimitOne() {
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
    if (obj instanceof QueryBuilder) {
      QueryBuilder that = (QueryBuilder) obj;
      return build().equals((that.build()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    return base.hashCode();
  }

  /**
   * Generates a new query to get MIN/MAX values for splitColumn.  
   * 
   * @param splitColumn column to use
   * @param minSplitColumnName MIN() column value alias
   * @param maxSplitColumnName MAX() column value alias
   * @return a new query builder
   */
  public QueryBuilder generateQueryToGetLimitsOfSplitColumn(
      String splitColumn,
      String minSplitColumnName,
      String maxSplitColumnName) {

    String selectMinMax = String.format(
            "SELECT MIN(%s) as %s, MAX(%s) as %s",
            splitColumn,
            minSplitColumnName,
            splitColumn,
            maxSplitColumnName);
    
    return new QueryBuilder(base.copyWithSelect(selectMinMax), this);
  }

}
