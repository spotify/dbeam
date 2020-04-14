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
import java.time.Instant;
import java.time.Period;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

public class PsqlReplicationCheckTest {
  private static String CONNECTION_URL =
      "jdbc:h2:mem:testpsql;MODE=postgresql;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1";

  private static JdbcExportArgs createArgs(String url, QueryBuilderArgs queryBuilderArgs)
      throws ClassNotFoundException {
    return JdbcExportArgs.create(
        JdbcAvroArgs.create(JdbcConnectionArgs.create(url)),
        queryBuilderArgs,
        "dbeam_generated",
        Optional.empty(),
        false,
        Duration.ZERO,
        Optional.empty());
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailOnInvalidDriver() throws ClassNotFoundException {
    JdbcExportArgs args = createArgs("jdbc:mysql://some_db", QueryBuilderArgs.create("some_table"));

    PsqlReplicationCheck.validateOptions(args);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailOnMissingPartition() throws ClassNotFoundException {
    JdbcExportArgs args =
        createArgs("jdbc:postgresql://some_db", QueryBuilderArgs.create("some_table"));

    PsqlReplicationCheck.validateOptions(args);
  }

  @Test
  public void shouldSucceedOnValidDriverAndPartition() throws ClassNotFoundException {
    final JdbcExportArgs args =
        createArgs(
            "jdbc:postgresql://some_db",
            QueryBuilderArgs.create("coffees")
                .builder()
                .setPartition(Instant.parse("2025-02-28T00:00:00Z"))
                .build());
    PsqlReplicationCheck.validateOptions(args);
  }

  @Test
  public void shouldBeNotReplicationDelayedWhenReplicatedUntilEndOfPartition() {
    Instant partition = Instant.parse("2027-07-31T00:00:00Z");
    Instant lastReplication = Instant.parse("2027-08-01T00:00:00Z");
    Period partitionPeriod = Period.ofDays(1);

    Assert.assertFalse(
        PsqlReplicationCheck.isReplicationDelayed(partition, lastReplication, partitionPeriod));
  }

  @Test
  public void shouldBeNotReplicationDelayedWhenReplicatedUntilEndTheNextDay() {
    Instant partition = Instant.parse("2027-07-31T00:00:00Z");
    Instant lastReplication = Instant.parse("2027-08-02T00:00:00Z");
    Period partitionPeriod = Period.ofDays(1);

    Assert.assertFalse(
        PsqlReplicationCheck.isReplicationDelayed(partition, lastReplication, partitionPeriod));
  }

  @Test
  public void shouldBeReplicationDelayedWhenReplicatedUpToPartitionStart() {
    Instant partition = Instant.parse("2027-07-31T00:00:00Z");
    Instant lastReplication = Instant.parse("2027-07-31T00:00:00Z");
    Period partitionPeriod = Period.ofDays(1);

    Assert.assertTrue(
        PsqlReplicationCheck.isReplicationDelayed(partition, lastReplication, partitionPeriod));
  }

  @Test
  public void shouldBeReplicationDelayedWhenReplicatedBeforePartitionStart() {
    Instant partition = Instant.parse("2027-07-31T00:00:00Z");
    Instant lastReplication = Instant.parse("2027-07-30T22:00:00Z");
    Period partitionPeriod = Period.ofDays(1);

    Assert.assertTrue(
        PsqlReplicationCheck.isReplicationDelayed(partition, lastReplication, partitionPeriod));
  }

  @Test
  public void shouldBeReplicationDelayedWhenReplicatedInsidePartition() {
    Instant partition = Instant.parse("2027-07-31T00:00:00Z");
    Instant lastReplication = Instant.parse("2027-07-31T23:59:59Z");
    Period partitionPeriod = Period.ofDays(1);

    Assert.assertTrue(
        PsqlReplicationCheck.isReplicationDelayed(partition, lastReplication, partitionPeriod));
  }

  @Test
  public void shouldWorkWithMonthlyPartitionPeriod() {
    Instant partition = Instant.parse("2027-07-31T00:00:00Z");
    Instant lastReplication = Instant.parse("2027-07-31T23:59:59Z");
    Period partitionPeriod = Period.ofMonths(1);

    Assert.assertTrue(
        PsqlReplicationCheck.isReplicationDelayed(partition, lastReplication, partitionPeriod));
  }

  @Test(expected = NotReadyException.class)
  public void shouldRunQueryAndReturnReplicationDelayed() throws Exception {
    String query =
        "SELECT parsedatetime('2017-02-01 23.58.57 UTC', 'yyyy-MM-dd HH.mm.ss z', 'en', 'UTC')"
            + " AS last_replication, "
            + "13 AS replication_delay";
    PsqlReplicationCheck replicationCheck =
        new PsqlReplicationCheck(
            createArgs(
                CONNECTION_URL,
                QueryBuilderArgs.create("coffees")
                    .builder()
                    .setPartition(Instant.parse("2025-02-28T00:00:00Z"))
                    .build()),
            query);
    Instant expectedLastReplication = Instant.parse("2017-02-01T23:58:57Z");

    Instant actual = replicationCheck.queryReplication();

    Assert.assertEquals(expectedLastReplication, actual);
    Assert.assertTrue(replicationCheck.isReplicationDelayed());
    replicationCheck.checkReplication();
  }

  @Test
  public void shouldRunQueryAndReturnReplicationNotDelayed() throws Exception {
    String query =
        "SELECT parsedatetime('2030-02-01 23.58.57 UTC', 'yyyy-MM-dd HH.mm.ss z', 'en', 'UTC')"
            + " AS last_replication, "
            + "13 AS replication_delay";
    PsqlReplicationCheck replicationCheck =
        new PsqlReplicationCheck(
            createArgs(
                CONNECTION_URL,
                QueryBuilderArgs.create("coffees")
                    .builder()
                    .setPartition(Instant.parse("2025-02-28T00:00:00Z"))
                    .build()),
            query);
    Instant expectedLastReplication = Instant.parse("2030-02-01T23:58:57Z");

    Instant actual = replicationCheck.queryReplication();

    Assert.assertEquals(expectedLastReplication, actual);
    Assert.assertFalse(replicationCheck.isReplicationDelayed());
    replicationCheck.checkReplication();
  }
}
