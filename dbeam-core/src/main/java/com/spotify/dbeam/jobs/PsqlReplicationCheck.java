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

package com.spotify.dbeam.jobs;

import com.google.api.client.util.Preconditions;

import com.spotify.dbeam.args.JdbcExportArgs;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.ReadablePeriod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PsqlReplicationCheck {
  private static final Logger LOGGER = LoggerFactory.getLogger(PsqlReplicationCheck.class);
  static final String REPLICATION_QUERY =
      "SELECT now() AS current_timestamp, "
          + "pg_last_xact_replay_timestamp() AS last_replication, "
          + "ROUND (( EXTRACT (EPOCH FROM now()) - "
          + "EXTRACT (EPOCH FROM pg_last_xact_replay_timestamp()) "
          + ") * 1000) AS replication_delay;";
  private final String replicationQuery;
  private final JdbcExportArgs jdbcExportArgs;

  public PsqlReplicationCheck(JdbcExportArgs jdbcExportArgs, String replicationQuery) {
    this.jdbcExportArgs = jdbcExportArgs;
    this.replicationQuery = replicationQuery;
  }

  public static PsqlReplicationCheck create(JdbcExportArgs jdbcExportArgs) {
    return new PsqlReplicationCheck(jdbcExportArgs, REPLICATION_QUERY);
  }

  static void validateOptions(JdbcExportArgs jdbcExportArgs) {
    Preconditions.checkArgument(
        jdbcExportArgs.jdbcAvroOptions().jdbcConnectionConfiguration()
            .driverClassName().contains("postgres"),
        "Must be a PostgreSQL connection");
    Preconditions.checkArgument(
        jdbcExportArgs.queryBuilderArgs().partition().isPresent(),
        "Partition parameter must be defined");
  }

  public void checkReplication() throws Exception {
    if (isReplicationDelayed()) {
      throw new NotReadyException("PostgreSQL replication is late");
    }
  }

  public boolean isReplicationDelayed() throws Exception {
    return isReplicationDelayed(
        this.jdbcExportArgs.queryBuilderArgs().partition().get(),
        queryReplication(),
        this.jdbcExportArgs.queryBuilderArgs().partitionPeriod());
  }

  static boolean isReplicationDelayed(DateTime partition, DateTime lastReplication,
                                      ReadablePeriod partitionPeriod) {
    if (lastReplication.isBefore(partition.plus(partitionPeriod))) {
      LOGGER.error("Replication was not completed for partition, "
              + "expected >= {}, actual = {}",
          partition.plus(partitionPeriod), lastReplication);
      return true;
    }
    return false;
  }

  static DateTime queryReplication(Connection connection, String query) throws SQLException {
    final ResultSet resultSet = connection.createStatement().executeQuery(query);
    Preconditions.checkState(resultSet.next(), "Replication query returned empty results");
    DateTime lastReplication = new DateTime(resultSet.getTimestamp("last_replication"));
    Duration replicationDelay = new Duration(resultSet.getLong("replication_delay"));
    LOGGER.info("Psql replication check lastReplication={} replicationDelay={}",
        lastReplication, replicationDelay);
    return lastReplication;
  }

  DateTime queryReplication() throws Exception {
    LOGGER.info("Checking PostgreSQL replication lag...");
    try (Connection connection = this.jdbcExportArgs.createConnection()) {
      return queryReplication(connection, replicationQuery);
    }
  }

}
