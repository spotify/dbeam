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

import com.spotify.dbeam.options.JdbcExportArgs
import org.joda.time.{DateTime, DateTimeZone, Days}
import org.scalatest._
import slick.jdbc.H2Profile.api._


class PsqlAvroJobTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  private val connectionUrl: String =
    "jdbc:h2:mem:testpsql;MODE=PostgreSQL;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1"
  private val db: Database = Database.forURL(connectionUrl, driver = "org.h2.Driver")
  private val connection: Connection = db.source.createConnection()

  override def beforeAll(): Unit = {
    JdbcTestFixtures.createFixtures(db, Seq(JdbcTestFixtures.record1))
  }

  it should "fail with invalid driver" in {
    val options = JdbcExportArgs(
      "com.mysql.jdbc.Driver",
      "jdbc:mysql://nonsense",
      "dbeam-extractor",
      "secret",
      "some_table",
      "/path",
      "dbeam_generated"
    )

    a[IllegalArgumentException] should be thrownBy {
      PsqlAvroJob.validateOptions(options)
    }
  }

  it should "fail with missing partition" in {
    val options = JdbcExportArgs(
      "org.postgresql.Driver",
      "jdbc:postgresql://nonsense",
      "dbeam-extractor",
      "secret",
      "some_table",
      "/path",
      "dbeam_generated"
    )

    a[IllegalArgumentException] should be thrownBy {
      PsqlAvroJob.validateOptions(options)
    }
  }

  it should "validate" in {
    val options = JdbcExportArgs(
      "org.postgresql.Driver",
      "jdbc:postgresql://nonsense",
      "dbeam-extractor",
      "secret",
      "some_table",
      "/path",
      "dbeam_generated",
      partition = Some(new DateTime(2027, 7, 31, 0, 0, DateTimeZone.UTC))
    )

    PsqlAvroJob.validateOptions(options)
  }

  it should "succeed replication state, replicated until end of partition" in {
    val partition = new DateTime(2027, 7, 31, 0, 0, DateTimeZone.UTC)
    val lastReplication = new DateTime(2027, 8, 1, 0, 0, DateTimeZone.UTC)
    val partitionPeriod = Days.ONE

    PsqlAvroJob.isReplicationDelayed(partition, lastReplication, partitionPeriod) shouldBe false
  }

  it should "succeed replication state, replicated until the next day" in {
    val partition = new DateTime(2027, 7, 31, 0, 0, DateTimeZone.UTC)
    val lastReplication = new DateTime(2027, 8, 2, 0, 0, DateTimeZone.UTC)
    val partitionPeriod = Days.ONE

    PsqlAvroJob.isReplicationDelayed(partition, lastReplication, partitionPeriod) shouldBe false
  }


  it should "fail on delayed replication, replicated up to partition start" in {
    val partition = new DateTime(2027, 7, 31, 0, 0, DateTimeZone.UTC)
    val lastReplication = new DateTime(2027, 7, 31, 0, 0, DateTimeZone.UTC)
    val partitionPeriod = Days.ONE

    PsqlAvroJob.isReplicationDelayed(partition, lastReplication, partitionPeriod) shouldBe true
  }

  it should "fail on delayed replication, last replicated before partition start" in {
    val partition = new DateTime(2027, 7, 31, 0, 0, DateTimeZone.UTC)
    val lastReplication = new DateTime(2027, 7, 30, 22, 0, DateTimeZone.UTC)
    val partitionPeriod = Days.ONE

    PsqlAvroJob.isReplicationDelayed(partition, lastReplication, partitionPeriod) shouldBe true
  }

  it should "fail on delayed replication, last replicated inside the partition" in {
    val partition = new DateTime(2027, 7, 31, 0, 0, DateTimeZone.UTC)
    val lastReplication = new DateTime(2027, 7, 31, 23, 59, 59, DateTimeZone.UTC)
    val partitionPeriod = Days.ONE

    PsqlAvroJob.isReplicationDelayed(partition, lastReplication, partitionPeriod) shouldBe true
  }

  it should "run query" in {
    val query = "SELECT " +
      "parsedatetime('2017-02-01 23.58.57 UTC', 'yyyy-MM-dd HH.mm.ss z', 'en', 'UTC')" +
      " AS last_replication, " +
      "13 AS replication_delay"
    val lastReplication = new DateTime(2017, 2, 1, 23, 58, 57, DateTimeZone.UTC)

    val actual = PsqlAvroJob.queryReplication(connection, query)

    new DateTime(actual, DateTimeZone.UTC) should be (lastReplication)
  }

}
