/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.dbeam

import java.sql.Connection

import com.spotify.scio.{Args, ScioContext}
import org.joda.time.{DateTime, Duration}
import org.slf4j.{Logger, LoggerFactory}

/**
  * PSQL extract to avro with pre check for replication lag
  */
object PsqlAvroJob {
  val log: Logger = LoggerFactory.getLogger(PsqlAvroJob.getClass)
  val PsqlReplicationQuery: String = """
    SELECT
    now() AS current_timestamp,
    pg_last_xact_replay_timestamp() AS last_replication,
    ROUND ((
        EXTRACT (EPOCH FROM now()) -
        EXTRACT (EPOCH FROM pg_last_xact_replay_timestamp())
    ) * 1000) AS replication_delay
    ;
"""

  def validateOptions(options: SqlAvroOptions): SqlAvroOptions = {
    require(options.driverClass.contains("postgres"), "Must be a PostgreSql connection")
    require(options.partition.isDefined, "Partition parameter must be defined")
    options
  }

  def queryReplication(connection: Connection, query: String = PsqlReplicationQuery): DateTime = {
    log.info("Checking replication lag...")
    try {
      val statement = connection.createStatement()
      val rs = statement.executeQuery(query)
      require(rs.next())
      val lastReplication = new DateTime(rs.getTimestamp("last_replication"))
      val replicationDelay = new Duration(rs.getLong("replication_delay"))
      log.info(s"Psql replication check lastReplication=${lastReplication} replicationDelay=${replicationDelay}")
      lastReplication
    } finally {
      if (connection != null)
        connection.close()
    }
  }

  def validateReplication(partition: DateTime, lastReplication: DateTime) = {
    if (lastReplication.isBefore(partition)) {
      log.error(s"Replication was not completed for partition, expected >= ${partition}, actual = ${lastReplication}")
      System.exit(20)
    }
    partition
  }

  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc: ScioContext, options: SqlAvroOptions) = SqlAvroOptions.contextAndOptions(cmdlineArgs)
    validateOptions(options)

    validateReplication(options.partition.get, queryReplication(options.createConnection()))

    JdbcAvroJob.main(cmdlineArgs)
  }

}
