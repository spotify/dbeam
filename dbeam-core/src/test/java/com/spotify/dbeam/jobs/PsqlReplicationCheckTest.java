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

package com.spotify.dbeam.jobs;

import com.spotify.dbeam.args.JdbcAvroArgs;
import com.spotify.dbeam.args.JdbcConnectionArgs;
import com.spotify.dbeam.args.JdbcExportArgs;
import com.spotify.dbeam.args.QueryBuilderArgs;

import java.time.Duration;
import java.util.Optional;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.junit.Assert;
import org.junit.Test;

public class PsqlReplicationCheckTest {
  private static String CONNECTION_URL =
      "jdbc:h2:mem:testpsql;MODE=postgresql;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1";

  private static JdbcExportArgs createArgs(String url,
                                    QueryBuilderArgs queryBuilderArgs)
      throws ClassNotFoundException {
    return JdbcExportArgs.create(
        JdbcAvroArgs.create(JdbcConnectionArgs.create(url)),
        queryBuilderArgs,
        "dbeam_generated", Optional.empty(), false,
        Duration.ZERO
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailOnInvalidDriver() throws ClassNotFoundException {
    JdbcExportArgs args = createArgs("jdbc:mysql://some_db",
                                     QueryBuilderArgs.create("some_table"));

    PsqlReplicationCheck.validateOptions(args);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailOnMissingPartition() throws ClassNotFoundException {
    JdbcExportArgs args = createArgs("jdbc:postgresql://some_db",
                                     QueryBuilderArgs.create("some_table"));

    PsqlReplicationCheck.validateOptions(args);
  }

  @Test
  public void shouldSucceedOnValidDriverAndPartition() throws ClassNotFoundException {
    final JdbcExportArgs args = createArgs("jdbc:postgresql://some_db",
                                              QueryBuilderArgs.create("coffees").builder()
                                                  .setPartition(
                                                      DateTime.parse("2025-02-28T00:00:00"))
                                                  .build());
    PsqlReplicationCheck.validateOptions(args);
  }

  @Test
  public void shouldBeNotReplicationDelayedWhenReplicatedUntilEndOfPartition() {
    DateTime partition = new DateTime(2027, 7, 31, 0, 0, DateTimeZone.UTC);
    DateTime lastReplication = new DateTime(2027, 8, 1, 0, 0, DateTimeZone.UTC);
    Days partitionPeriod = Days.ONE;

    Assert.assertFalse(
        PsqlReplicationCheck.isReplicationDelayed(partition, lastReplication, partitionPeriod));
  }

  @Test
  public void shouldBeNotReplicationDelayedWhenReplicatedUntilEndTheNextDay() {
    DateTime partition = new DateTime(2027, 7, 31, 0, 0, DateTimeZone.UTC);
    DateTime lastReplication = new DateTime(2027, 8, 2, 0, 0, DateTimeZone.UTC);
    Days partitionPeriod = Days.ONE;

    Assert.assertFalse(
        PsqlReplicationCheck.isReplicationDelayed(partition, lastReplication, partitionPeriod));
  }

  @Test
  public void shouldBeReplicationDelayedWhenReplicatedUpToPartitionStart() {
    DateTime partition = new DateTime(2027, 7, 31, 0, 0, DateTimeZone.UTC);
    DateTime lastReplication = new DateTime(2027, 7, 31, 0, 0, DateTimeZone.UTC);
    Days partitionPeriod = Days.ONE;

    Assert.assertTrue(
        PsqlReplicationCheck.isReplicationDelayed(partition, lastReplication, partitionPeriod));
  }

  @Test
  public void shouldBeReplicationDelayedWhenReplicatedBeforePartitionStart() {
    DateTime partition = new DateTime(2027, 7, 31, 0, 0, DateTimeZone.UTC);
    DateTime lastReplication = new DateTime(2027, 7, 30, 22, 0, DateTimeZone.UTC);
    Days partitionPeriod = Days.ONE;

    Assert.assertTrue(
        PsqlReplicationCheck.isReplicationDelayed(partition, lastReplication, partitionPeriod));
  }

  @Test
  public void shouldBeReplicationDelayedWhenReplicatedInsidePartition() {
    DateTime partition = new DateTime(2027, 7, 31, 0, 0, DateTimeZone.UTC);
    DateTime lastReplication = new DateTime(2027, 7, 31, 23, 59, 59, DateTimeZone.UTC);
    Days partitionPeriod = Days.ONE;

    Assert.assertTrue(
        PsqlReplicationCheck.isReplicationDelayed(partition, lastReplication, partitionPeriod));
  }

  @Test(expected = NotReadyException.class)
  public void shouldRunQueryAndReturnReplicationDelayed() throws Exception {
    String query =
        "SELECT parsedatetime('2017-02-01 23.58.57 UTC', 'yyyy-MM-dd HH.mm.ss z', 'en', 'UTC')"
        + " AS last_replication, "
        + "13 AS replication_delay";
    PsqlReplicationCheck replicationCheck = new PsqlReplicationCheck(
        createArgs(CONNECTION_URL,
                         QueryBuilderArgs.create("coffees").builder()
                                             .setPartition(DateTime.parse("2025-02-28T00:00:00"))
                                             .build()), query);
    DateTime expectedLastReplication = new DateTime(2017, 2, 1, 23, 58, 57, DateTimeZone.UTC);

    DateTime actual = replicationCheck.queryReplication();

    Assert.assertEquals(
        expectedLastReplication,
        new DateTime(actual, DateTimeZone.UTC)
    );
    Assert.assertTrue(replicationCheck.isReplicationDelayed());
    replicationCheck.checkReplication();
  }

  @Test
  public void shouldRunQueryAndReturnReplicationNotDelayed() throws Exception {
    String query =
        "SELECT parsedatetime('2030-02-01 23.58.57 UTC', 'yyyy-MM-dd HH.mm.ss z', 'en', 'UTC')"
        + " AS last_replication, "
        + "13 AS replication_delay";
    PsqlReplicationCheck replicationCheck = new PsqlReplicationCheck(
        createArgs(CONNECTION_URL,
                         QueryBuilderArgs.create("coffees").builder()
                                             .setPartition(DateTime.parse("2025-02-28T00:00:00"))
                                             .build()), query);
    DateTime expectedLastReplication = new DateTime(2030, 2, 1, 23, 58, 57, DateTimeZone.UTC);

    DateTime actual = replicationCheck.queryReplication();

    Assert.assertEquals(
        expectedLastReplication,
        new DateTime(actual, DateTimeZone.UTC)
    );
    Assert.assertFalse(replicationCheck.isReplicationDelayed());
    replicationCheck.checkReplication();
  }

}
