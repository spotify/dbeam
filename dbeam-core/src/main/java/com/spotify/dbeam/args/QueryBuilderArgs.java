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
import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Optional;

/** A POJO describing how to create queries for DBeam exports. */
@AutoValue
public abstract class QueryBuilderArgs implements Serializable {

  abstract QueryBuilder baseSqlQuery();

  public abstract Optional<Long> limit();

  public abstract Optional<String> partitionColumn();

  public abstract Optional<Instant> partition();

  public abstract TemporalAmount partitionPeriod();

  public abstract Optional<String> splitColumn();

  public abstract Optional<Integer> queryParallelism();

  public abstract Builder builder();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setBaseSqlQuery(QueryBuilder baseSqlQuery);

    public abstract Builder setLimit(Long limit);

    public abstract Builder setLimit(Optional<Long> limit);

    public abstract Builder setPartitionColumn(String partitionColumn);

    public abstract Builder setPartitionColumn(Optional<String> partitionColumn);

    public abstract Builder setPartition(Instant partition);

    public abstract Builder setPartition(Optional<Instant> partition);

    public abstract Builder setPartitionPeriod(TemporalAmount partitionPeriod);

    public abstract Builder setSplitColumn(String splitColumn);

    public abstract Builder setSplitColumn(Optional<String> splitColumn);

    public abstract Builder setQueryParallelism(Integer parallelism);

    public abstract Builder setQueryParallelism(Optional<Integer> queryParallelism);

    public abstract QueryBuilderArgs build();
  }

  private static Boolean checkIfValidDatabaseObjectName(final String objectName) {
    return objectName.matches("^[a-zA-Z_][a-zA-Z0-9_]*$");
  }

  private static Boolean checkSchemaName(final String dbSchemaName) {
    return dbSchemaName == null || dbSchemaName.matches("^[a-zA-Z_][a-zA-Z0-9_]*$");
  }


  private static Builder createBuilder() {
    return new AutoValue_QueryBuilderArgs.Builder().setPartitionPeriod(Period.ofDays(1));
  }

  public static QueryBuilderArgs create(final String tableName) {
    return create(null,tableName);
  }

  public static QueryBuilderArgs create(final String dbSchemaName, final String tableName) {
    checkArgument(tableName != null, "TableName cannot be null");
    checkArgument(checkIfValidDatabaseObjectName(tableName),
            "'table' must follow [a-zA-Z_][a-zA-Z0-9_]*");
    checkArgument(checkSchemaName(dbSchemaName),
            "'dbSchema' must follow [a-zA-Z_][a-zA-Z0-9_]*");

    return createBuilder().setBaseSqlQuery(
            QueryBuilder.fromTablename(dbSchemaName,tableName)).build();
  }

  public static QueryBuilderArgs createFromQuery(final String sqlQuery) {
    return createBuilder().setBaseSqlQuery(QueryBuilder.fromSqlQuery(sqlQuery)).build();
  }

  /**
   * Returns query with limit one, so it can be used to query and fetch schema.
   *
   * @return
   */
  public String sqlQueryWithLimitOne() {
    return this.baseSqlQuery().withLimit(1L).build();
  }

  /**
   * Create queries to be executed for the export job.
   *
   * @param connection A connection which is used to determine limits for parallel queries.
   * @return A list of queries to be executed.
   * @throws SQLException when it fails to find out limits for splits.
   */
  public List<String> buildQueries(final Connection connection) throws SQLException {
    QueryBuilder queryBuilder = this.baseSqlQuery();
    if (this.partitionColumn().isPresent() && this.partition().isPresent()) {
      queryBuilder = configurePartitionCondition(
          this.partitionColumn().get(),
          this.partition().get(),
          partitionPeriod(),
          queryBuilder);
    }
    if (this.limit().isPresent()) {
      queryBuilder = queryBuilder
          .withLimit(queryParallelism().map(k -> limit().get() / k).orElse(limit().get()));
    }

    if (queryParallelism().isPresent() && splitColumn().isPresent()) {
      long[] minMax = findInputBounds(connection, queryBuilder, splitColumn().get());
      long min = minMax[0];
      long max = minMax[1];

      return queriesForBounds(
          min, max, queryParallelism().get(), splitColumn().get(), queryBuilder);
    } else {
      return Lists.newArrayList(queryBuilder.build());
    }
  }

  private QueryBuilder configurePartitionCondition(final String partitionColumn,
                                                   final Instant partition,
                                                   final TemporalAmount partitionPeriod,
                                                   final QueryBuilder queryBuilder) {
    if (partitionPeriod() instanceof Period) {
      final LocalDate partitionDate = partition.atZone(ZoneOffset.UTC).toLocalDate();
      final LocalDate nextPartition = partitionDate.plus(partitionPeriod);
      return queryBuilder.withPartitionCondition(
          partitionColumn,
          partitionDate.toString(),
          nextPartition.toString());
    } else {
      // in case of sub daily period, use the full timestamp
      final Instant nextPartition = partition.plus(partitionPeriod);
      return queryBuilder.withPartitionCondition(
              partitionColumn, partition.toString(), nextPartition.toString());
    }
  }

}
