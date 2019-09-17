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

import static com.google.common.base.Preconditions.checkArgument;
import static com.spotify.dbeam.args.ParallelQueryBuilder.findInputBounds;
import static com.spotify.dbeam.args.ParallelQueryBuilder.queriesForBounds;

import com.google.auto.value.AutoValue;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.joda.time.ReadablePeriod;

/**
 * A POJO describing how to create queries for DBeam exports.
 */
@AutoValue
public abstract class QueryBuilderArgs implements Serializable {

  public abstract String tableName();

  public abstract QueryBuilder baseSqlQuery();

  public abstract Optional<Long> limit();

  public abstract Optional<String> partitionColumn();

  public abstract Optional<DateTime> partition();

  public abstract ReadablePeriod partitionPeriod();

  public abstract Optional<String> splitColumn();

  public abstract Optional<Integer> queryParallelism();

  public abstract Builder builder();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setTableName(String tableName);

    public abstract Builder setBaseSqlQuery(QueryBuilder baseSqlQuery);

    public abstract Builder setLimit(Long limit);

    public abstract Builder setLimit(Optional<Long> limit);

    public abstract Builder setPartitionColumn(String partitionColumn);

    public abstract Builder setPartitionColumn(Optional<String> partitionColumn);

    public abstract Builder setPartition(DateTime partition);

    public abstract Builder setPartition(Optional<DateTime> partition);

    public abstract Builder setPartitionPeriod(ReadablePeriod partitionPeriod);

    public abstract Builder setSplitColumn(String splitColumn);

    public abstract Builder setSplitColumn(Optional<String> splitColumn);

    public abstract Builder setQueryParallelism(Integer parallelism);

    public abstract Builder setQueryParallelism(Optional<Integer> queryParallelism);

    public abstract QueryBuilderArgs build();
  }

  private static Boolean checkTableName(String tableName) {
    return tableName.matches("^[a-zA-Z_][a-zA-Z0-9_]*$");
  }

  private static Builder builderForTableName(String tableName) {
    checkArgument(tableName != null, "TableName cannot be null");
    checkArgument(checkTableName(tableName), "'table' must follow [a-zA-Z_][a-zA-Z0-9_]*");

    return new AutoValue_QueryBuilderArgs.Builder()
        .setTableName(tableName)
        .setBaseSqlQuery(QueryBuilder.fromTablename(tableName))
        .setPartitionPeriod(Days.ONE);
  }

  public static QueryBuilderArgs create(String tableName) {
    return QueryBuilderArgs.builderForTableName(tableName).build();
  }

  public static QueryBuilderArgs create(String tableName, String sqlQueryOpt) {
    return QueryBuilderArgs.builderForTableName(tableName)
        .setBaseSqlQuery(QueryBuilder.fromSqlQuery(sqlQueryOpt))
        .build();
  }

  /**
   * Create queries to be executed for the export job.
   *
   * @param connection A connection which is used to determine limits for parallel queries.
   * @return A list of queries to be executed.
   * @throws SQLException when it fails to find out limits for splits.
   */
  public List<String> buildQueries(Connection connection)
      throws SQLException {
    this.partitionColumn()
        .ifPresent(
            partitionColumn ->
                this.partition()
                    .ifPresent(
                        partition -> {
                          final LocalDate datePartition = partition.toLocalDate();
                          final String nextPartition =
                              datePartition.plus(partitionPeriod()).toString();
                          this.baseSqlQuery()
                              .withPartitionCondition(
                                  partitionColumn, datePartition.toString(), nextPartition);
                        }));
    this.limit()
        .ifPresent(l ->
                       this.baseSqlQuery().withLimit(queryParallelism().map(k -> l / k).orElse(l)));

    if (queryParallelism().isPresent() && splitColumn().isPresent()) {
      long[] minMax = findInputBounds(connection, this.baseSqlQuery(), splitColumn().get());
      long min = minMax[0];
      long max = minMax[1];

      return queriesForBounds(
          min, max, queryParallelism().get(), splitColumn().get(), this.baseSqlQuery());
    } else {
      return Lists.newArrayList(this.baseSqlQuery().build());
    }
  }

}
