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

import com.google.auto.value.AutoValue;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.joda.time.ReadablePeriod;

/**
 * A POJO describing a how to create a JDBC {@link Connection}.
 */
@AutoValue
public abstract class QueryBuilderArgs implements Serializable {

  public abstract String tableName();

  public abstract Optional<Integer> limit();

  public abstract Optional<String> partitionColumn();

  public abstract Optional<DateTime> partition();

  public abstract ReadablePeriod partitionPeriod();

  public abstract Optional<String> splitColumn();

  public abstract Optional<Integer> parallelism();

  public abstract Builder builder();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setTableName(String tableName);

    public abstract Builder setLimit(Integer limit);

    public abstract Builder setLimit(Optional<Integer> limit);

    public abstract Builder setPartitionColumn(String partitionColumn);

    public abstract Builder setPartitionColumn(Optional<String> partitionColumn);

    public abstract Builder setPartition(DateTime partition);

    public abstract Builder setPartition(Optional<DateTime> partition);

    public abstract Builder setPartitionPeriod(ReadablePeriod partitionPeriod);

    public abstract Builder setSplitColumn(String splitColumn);

    public abstract Builder setSplitColumn(Optional<String> splitColumn);

    public abstract Builder setParallelism(Integer parallelism);

    public abstract Builder setParallelism(Optional<Integer> parallelism);


    public abstract QueryBuilderArgs build();
  }

  private static Boolean checkTableName(String tableName) {
    return tableName.matches("^[a-zA-Z_][a-zA-Z0-9_]*$");
  }

  public static QueryBuilderArgs create(String tableName) {
    checkArgument(tableName != null,
        "TableName cannot be null");
    checkArgument(checkTableName(tableName),
        "'table' must follow [a-zA-Z_][a-zA-Z0-9_]*");
    return new AutoValue_QueryBuilderArgs.Builder()
        .setTableName(tableName)
        .setPartitionPeriod(Days.ONE)
        .build();
  }

  public Iterable<String> buildQueries(Connection connection)
      throws SQLException {
    checkArgument(parallelism().isPresent() || !splitColumn().isPresent(),
        "Cannot use parallelism because no column to split is specified. "
            + "Please specify column to use for splitting using --splitColumn");
    checkArgument(!parallelism().isPresent() || splitColumn().isPresent(),
        "argument splitColumn has no effect since --parallelism is not specified");

    final String limit = this.limit().map(l -> String.format(" LIMIT %d", l)).orElse("");

    final String where = " WHERE 1=1";
    final String partitionCondition = this.partitionColumn().flatMap(
        partitionColumn ->
            this.partition().map(partition -> {
              final LocalDate datePartition = partition.toLocalDate();
              final String nextPartition = datePartition.plus(partitionPeriod()).toString();
              return String.format(" AND %s >= '%s' AND %s < '%s'",
                  partitionColumn, datePartition, partitionColumn, nextPartition);
            })
    ).orElse("");

    if (parallelism().isPresent() && splitColumn().isPresent()) {
      // Generate queries to get limits of split column.
      String query = String.format(
          "SELECT min(%s) as min_s, max(%s) as max_s FROM %s%s%s",
          splitColumn().get(),
          splitColumn().get(),
          this.tableName(),
          where,
          partitionCondition);
      long min;
      long max;
      try (Statement statement = connection.createStatement()) {
        final ResultSet
            resultSet =
            statement.executeQuery(query);
        resultSet.first();

        // min_s and max_s would both of the same type
        switch (resultSet.getMetaData().getColumnType(1)) {
          case Types.LONGVARBINARY:
          case Types.BIGINT:
            min = resultSet.getLong("min_s");
            max = resultSet.getLong("max_s");
            break;
          case Types.INTEGER:
            min = resultSet.getInt("min_s");
            max = resultSet.getInt("max_s");
            break;
          default:
            throw new IllegalArgumentException("splitColumn should be of type Integer / Long");
        }
      }

      long bucketSize = (max - min) / parallelism().get();
      bucketSize =
          bucketSize == 0 ? 1 : bucketSize; // If parallelism is too high, this would result in 0
      String limitWithParallelism = this.limit()
          .map(l -> String.format(" LIMIT %d", l / parallelism().get())).orElse("");
      List<String> queries = new ArrayList<>(parallelism().get());

      max++; // We would use < maxL in our queries.
      long i = min;
      while (i < max) {
        String parallelismCondtion = String
            .format(" AND %s >= %s AND %s < %s",
                splitColumn().get(),
                i,
                splitColumn().get(),
                i + bucketSize);

        queries.add(String
            .format("SELECT * FROM %s%s%s%s%s", this.tableName(),
                where,
                partitionCondition,
                parallelismCondtion,
                limitWithParallelism));
        i = i + bucketSize;
      }
      return queries;
    } else {
      return Lists.newArrayList(
          String.format("SELECT * FROM %s%s%s%s", this.tableName(), where, partitionCondition,
              limit));
    }
  }

}
