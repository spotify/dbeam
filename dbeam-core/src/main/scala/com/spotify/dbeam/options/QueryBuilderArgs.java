/*
 * Copyright 2018 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.spotify.dbeam.options;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.joda.time.Period;
import org.joda.time.ReadablePeriod;
import org.joda.time.format.ISODateTimeFormat;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Optional;

import javax.annotation.Nullable;

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
    public abstract QueryBuilderArgs build();
  }

  private static DateTime parseDateTime(String input) {
    if (input.endsWith("Z")) {
      input = input.substring(0, input.length() -1);
    }
    return DateTime.parse(input, ISODateTimeFormat.localDateOptionalTimeParser());
  }

  private static DateTime validatePartition(DateTime partitionDateTime, DateTime minPartitionDateTime) {
    Preconditions.checkArgument(
        partitionDateTime.isAfter(minPartitionDateTime),
        "Too old partition date %s. Use a partition date >= %s or use --skip-partition-check",
        partitionDateTime, minPartitionDateTime
    );
    return partitionDateTime;
  }

  private static Boolean checkTableName(String tableName) {
    return tableName.matches("^[a-zA-Z_][a-zA-Z0-9_]*$");
  }

  public static QueryBuilderArgs create(JdbcExportPipelineOptions options) {
    Preconditions.checkArgument(options.getTable() != null,
    "'table' must be defined");
    Preconditions.checkArgument(checkTableName(options.getTable()),
                                "'table' must follow [a-zA-Z_][a-zA-Z0-9_]*");
    final ReadablePeriod partitionPeriod = Optional.ofNullable(options.getPartitionPeriod())
        .map(v -> (ReadablePeriod) Period.parse(v)).orElse(Days.ONE);
    Optional<DateTime> partition = Optional.ofNullable(options.getPartition())
        .map(QueryBuilderArgs::parseDateTime);
    Optional<String> partitionColumn = Optional.ofNullable(options.getPartitionColumn());
    Preconditions.checkArgument(!partitionColumn.isPresent() || partition.isPresent(),
                                "To use --partitionColumn the --partition parameter must also be configured");

    if (!(options.isSkipPartitionCheck() || partitionColumn.isPresent())) {
      DateTime minPartitionDateTime = Optional.ofNullable(options.getMinPartitionPeriod())
          .map(QueryBuilderArgs::parseDateTime)
          .orElse(DateTime.now().minus(partitionPeriod.toPeriod().multipliedBy(2)));
      partition.map(p -> validatePartition(p, minPartitionDateTime));
    }
    return new AutoValue_QueryBuilderArgs.Builder()
        .setTableName(options.getTable())
        .setLimit(Optional.ofNullable(options.getLimit()))
        .setPartitionColumn(partitionColumn)
        .setPartition(partition)
        .setPartitionPeriod(partitionPeriod)
        .build();
  }

  public static QueryBuilderArgs create(String tableName) {
    Preconditions.checkArgument(tableName != null,
                                "TableName cannot be null");
    return new AutoValue_QueryBuilderArgs.Builder()
        .setTableName(tableName)
        .setPartitionPeriod(Days.ONE)
        .build();
  }


  public Iterable<String> buildQueries() {
    final String limit = this.limit().map(l -> String.format(" LIMIT %d", l)).orElse("");
    final String where = this.partitionColumn().flatMap(
        partitionColumn ->
            this.partition().map(partition -> {
              final LocalDate datePartition = partition.toLocalDate();
              final String nextPartition = datePartition.plus(partitionPeriod()).toString();
              return String.format(" WHERE %s >= '%s' AND %s < '%s'",
                                   partitionColumn, datePartition, partitionColumn, nextPartition);
            })
    ).orElse("");
    return Lists.newArrayList(String.format("SELECT * FROM %s%s%s", this.tableName(), where, limit));
  }

}
