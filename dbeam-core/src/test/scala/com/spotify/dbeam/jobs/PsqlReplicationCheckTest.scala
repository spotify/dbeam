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

package com.spotify.dbeam.jobs

import com.spotify.dbeam.JdbcTestFixtures
import com.spotify.dbeam.args.{JdbcAvroArgs, JdbcConnectionArgs, JdbcExportArgs, QueryBuilderArgs}
import com.spotify.dbeam.options.OptionsParser
import org.joda.time.{DateTime, DateTimeZone, Days}
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import slick.jdbc.H2Profile.api._


@RunWith(classOf[JUnitRunner])
class PsqlReplicationCheckTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  private val connectionUrl: String =
    "jdbc:h2:mem:testpsql;MODE=postgresql;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1"
  private val db: Database = Database.forURL(connectionUrl, driver = "org.h2.Driver")

  override def beforeAll(): Unit = {
    JdbcTestFixtures.createFixtures(db, Seq(JdbcTestFixtures.record1))
  }

  it should "fail with invalid driver" in {
    val args = JdbcExportArgs.create(
      JdbcAvroArgs.create(
        JdbcConnectionArgs.create("jdbc:mysql://some_db")
          .withUsername("dbeam-extractor")
          .withPassword("secret")
      ),
      QueryBuilderArgs.create("some_table")
    )

    a[IllegalArgumentException] should be thrownBy {
      PsqlReplicationCheck.validateOptions(args)
    }
  }

  it should "fail with missing partition" in {
    val args = JdbcExportArgs.create(
      JdbcAvroArgs.create(
        JdbcConnectionArgs.create("jdbc:postgresql://some_db")
          .withUsername("dbeam-extractor")
          .withPassword("secret")
      ),
      QueryBuilderArgs.create("some_table")
    )

    a[IllegalArgumentException] should be thrownBy {
      PsqlReplicationCheck.validateOptions(args)
    }
  }

  it should "validate" in {
    val args = JdbcExportArgs.create(
      JdbcAvroArgs.create(
        JdbcConnectionArgs.create("jdbc:postgresql://some_db")
          .withUsername("dbeam-extractor")
          .withPassword("secret")
      ),
      QueryBuilderArgs.create("some_table")
        .builder()
        .setPartition(new DateTime(2027, 7, 31, 0, 0, DateTimeZone.UTC)).build()
    )

    PsqlReplicationCheck.validateOptions(args)
  }

  it should "succeed replication state, replicated until end of partition" in {
    val partition = new DateTime(2027, 7, 31, 0, 0, DateTimeZone.UTC)
    val lastReplication = new DateTime(2027, 8, 1, 0, 0, DateTimeZone.UTC)
    val partitionPeriod = Days.ONE

    PsqlReplicationCheck.isReplicationDelayed(
      partition, lastReplication, partitionPeriod) shouldBe false
  }

  it should "succeed replication state, replicated until the next day" in {
    val partition = new DateTime(2027, 7, 31, 0, 0, DateTimeZone.UTC)
    val lastReplication = new DateTime(2027, 8, 2, 0, 0, DateTimeZone.UTC)
    val partitionPeriod = Days.ONE

    PsqlReplicationCheck.isReplicationDelayed(
      partition, lastReplication, partitionPeriod) shouldBe false
  }


  it should "fail on delayed replication, replicated up to partition start" in {
    val partition = new DateTime(2027, 7, 31, 0, 0, DateTimeZone.UTC)
    val lastReplication = new DateTime(2027, 7, 31, 0, 0, DateTimeZone.UTC)
    val partitionPeriod = Days.ONE

    PsqlReplicationCheck.isReplicationDelayed(
      partition, lastReplication, partitionPeriod) shouldBe true
  }

  it should "fail on delayed replication, last replicated before partition start" in {
    val partition = new DateTime(2027, 7, 31, 0, 0, DateTimeZone.UTC)
    val lastReplication = new DateTime(2027, 7, 30, 22, 0, DateTimeZone.UTC)
    val partitionPeriod = Days.ONE

    PsqlReplicationCheck.isReplicationDelayed(
      partition, lastReplication, partitionPeriod) shouldBe true
  }

  it should "fail on delayed replication, last replicated inside the partition" in {
    val partition = new DateTime(2027, 7, 31, 0, 0, DateTimeZone.UTC)
    val lastReplication = new DateTime(2027, 7, 31, 23, 59, 59, DateTimeZone.UTC)
    val partitionPeriod = Days.ONE

    PsqlReplicationCheck.isReplicationDelayed(
      partition, lastReplication, partitionPeriod) shouldBe true
  }

  it should "run query and return replication delayed" in {
    val query = "SELECT " +
      "parsedatetime('2017-02-01 23.58.57 UTC', 'yyyy-MM-dd HH.mm.ss z', 'en', 'UTC')" +
      " AS last_replication, " +
      "13 AS replication_delay"
    val replicationCheck = new PsqlReplicationCheck(
      JdbcExportArgs.create(
        JdbcAvroArgs.create(
          JdbcConnectionArgs.create(connectionUrl)),
        QueryBuilderArgs.create("coffees").builder()
          .setPartition(DateTime.parse("2025-02-28T00:00:00"))
          .build()),
      query
    )
    val lastReplication = new DateTime(2017, 2, 1, 23, 58, 57, DateTimeZone.UTC)

    val actual = replicationCheck.queryReplication()

    new DateTime(actual, DateTimeZone.UTC) should be (lastReplication)
    replicationCheck.isReplicationDelayed shouldBe true
    a[NotReadyException] should be thrownBy {
      replicationCheck.checkReplication()
    }
  }

  it should "run query and return replication not delayed" in {
    val query = "SELECT " +
      "parsedatetime('2030-02-01 23.58.57 UTC', 'yyyy-MM-dd HH.mm.ss z', 'en', 'UTC')" +
      " AS last_replication, " +
      "13 AS replication_delay"
    val replicationCheck = new PsqlReplicationCheck(
      JdbcExportArgs.create(
        JdbcAvroArgs.create(
          JdbcConnectionArgs.create(connectionUrl)),
        QueryBuilderArgs.create("coffees").builder()
          .setPartition(DateTime.parse("2025-02-28T00:00:00"))
          .build()),
      query
    )
    val lastReplication = new DateTime(2030, 2, 1, 23, 58, 57, DateTimeZone.UTC)

    val actual = replicationCheck.queryReplication()

    new DateTime(actual, DateTimeZone.UTC) should be (lastReplication)
    replicationCheck.isReplicationDelayed shouldBe false
    noException should be thrownBy {
      replicationCheck.checkReplication()
    }
  }

}
