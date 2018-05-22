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

package com.spotify.dbeam.options

import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.joda.time.{DateTime, DateTimeZone, Period}
import org.scalatest._

class JdbcExportArgsTest extends FlatSpec with Matchers {

  def optionsFromArgs(cmdLineArgs: String): JdbcExportArgs = {
    PipelineOptionsFactory.register(classOf[JdbcExportPipelineOptions])
    val opts: PipelineOptions =
      PipelineOptionsFactory.fromArgs(cmdLineArgs.split(" "):_*).withValidation().create()
    JdbcExportArgs.fromPipelineOptions(opts)
  }

  it should "fail parse invalid arguments" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("")
      optionsFromArgs("--foo=bar")
    }
  }
  it should "fail to parse with missing connectionUrl parameter" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--table=some-table")
    }
  }
  it should "fail to parse with missing table parameter" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense")
    }
  }
  it should "parse correctly with missing password parameter" in {
    val options = optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense --table=some_table")

    options should be (JdbcExportArgs(
      "org.postgresql.Driver",
      "jdbc:postgresql://nonsense",
      "dbeam-extractor",
      null,
      "some_table",
      "dbeam_generated",
      None,
      None,
      None
    ))
  }
  it should "fail to parse invalid table parameter" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense --table=some-table " +
        "--password=secret")
    }
  }
  it should "fail to parse non jdbc connection parameter" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=bar --table=some_table --password=secret")
    }
  }
  it should "fail to parse unsupported jdbc connection parameter" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=jdbc:paradox:./foo --table=some_table " +
        "--password=secret")
    }
  }
  it should "fail to parse missing partition parameter but partition column present" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense --table=some_table " +
        "--password=secret --partitionColumn=col")
    }
  }
  it should "fail on too old partition parameter" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense --table=some_table " +
        "--password=secret --partition=2015-01-01")
    }
  }
  it should "fail on old partition parameter with configured partition = min-partition-period" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense --table=some_table " +
        "--password=secret --partition=2015-01-01 --minPartitionPeriod=2015-01-01")
    }
  }
  it should "fail on old partition parameter with configured partition < min-partition-period" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense --table=some_table " +
        "--password=secret --partition=2015-01-01 --minPartitionPeriod=2015-01-02")
    }
  }
  it should "parse correctly for postgresql connection" in {
    val options = optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense --table=some_table " +
      "--password=secret")

    options should be (JdbcExportArgs(
      "org.postgresql.Driver",
      "jdbc:postgresql://nonsense",
      "dbeam-extractor",
      "secret",
      "some_table",
      "dbeam_generated",
      None,
      None,
      None
    ))
  }
  it should "parse correctly for mysql connection" in {
    val options = optionsFromArgs("--connectionUrl=jdbc:mysql://nonsense --table=some_table " +
      "--password=secret")

    options should be (JdbcExportArgs(
      "com.mysql.jdbc.Driver",
      "jdbc:mysql://nonsense",
      "dbeam-extractor",
      "secret",
      "some_table",
      "dbeam_generated",
      None,
      None,
      None
    ))
  }
  it should "configure username" in {
    val options = optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense " +
      "--table=some_table --password=secret --username=some_user")

    options should be (JdbcExportArgs(
      "org.postgresql.Driver",
      "jdbc:postgresql://nonsense",
      "some_user",
      "secret",
      "some_table",
      "dbeam_generated",
      None,
      None,
      None
    ))
  }
  it should "configure limit" in {
    val actual = optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense " +
      "--table=some_table --password=secret --limit=7")

    val expected = JdbcExportArgs(
      "org.postgresql.Driver",
      "jdbc:postgresql://nonsense",
      "dbeam-extractor",
      "secret",
      "some_table",
      "dbeam_generated",
      Some(7),
      None,
      None
    )
    actual should be (expected)
    actual.buildQueries() should be (Seq("SELECT * FROM some_table LIMIT 7"))
  }
  it should "configure partition" in {
    val actual = optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense " +
      "--table=some_table --password=secret --partition=2027-07-31")

    val expected = JdbcExportArgs(
      "org.postgresql.Driver",
      "jdbc:postgresql://nonsense",
      "dbeam-extractor",
      "secret",
      "some_table",
      "dbeam_generated",
      None,
      None,
      Some(new DateTime(2027, 7, 31, 0, 0, DateTimeZone.UTC))
    )
    actual should be (expected)
    actual.buildQueries() should be (Seq("SELECT * FROM some_table"))
  }
  it should "configure partition with full ISO date time (Styx cron syntax)" in {
    val actual = optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense --table=some_table " +
      "--password=secret --partition=2027-07-31T13:37:59Z")

    val expected = JdbcExportArgs(
      "org.postgresql.Driver",
      "jdbc:postgresql://nonsense",
      "dbeam-extractor",
      "secret",
      "some_table",
      "dbeam_generated",
      None,
      None,
      Some(new DateTime(2027, 7, 31, 13, 37, 59, DateTimeZone.UTC))
    )
    actual should be (expected)
    actual.buildQueries() should be (Seq("SELECT * FROM some_table"))
  }
  it should "configure partition with month date (Styx monthly schedule)" in {
    val actual = optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense " +
      "--table=some_table --password=secret --partition=2027-05")

    val expected = JdbcExportArgs(
      "org.postgresql.Driver",
      "jdbc:postgresql://nonsense",
      "dbeam-extractor",
      "secret",
      "some_table",
      "dbeam_generated",
      None,
      None,
      Some(new DateTime(2027, 5, 1, 0, 0, 0, DateTimeZone.UTC))
    )
    actual should be (expected)
    actual.buildQueries() should be (Seq("SELECT * FROM some_table"))
  }
  it should "configure partition column" in {
    val actual = optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense --table=some_table " +
      "--password=secret --partition=2027-07-31 --partitionColumn=col")

    val expected = JdbcExportArgs(
      "org.postgresql.Driver",
      "jdbc:postgresql://nonsense",
      "dbeam-extractor",
      "secret",
      "some_table",
      "dbeam_generated",
      None,
      Some("col"),
      Some(new DateTime(2027, 7, 31, 0, 0, 0, DateTimeZone.UTC))
    )
    actual should be (expected)
    actual.buildQueries() should be (Seq("SELECT * FROM some_table " +
      "WHERE col >= '2027-07-31' AND col < '2027-08-01'"))
  }
  it should "configure partition column and limit" in {
    val actual = optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense --table=some_table " +
      "--password=secret --partition=2027-07-31 --partitionColumn=col --limit=5")

    val expected = JdbcExportArgs(
      "org.postgresql.Driver",
      "jdbc:postgresql://nonsense",
      "dbeam-extractor",
      "secret",
      "some_table",
      "dbeam_generated",
      Some(5),
      Some("col"),
      Some(new DateTime(2027, 7, 31, 0, 0, 0, DateTimeZone.UTC))
    )
    actual should be (expected)
    actual.buildQueries() should be (Seq("SELECT * FROM some_table WHERE col >= '2027-07-31'" +
      " AND col < '2027-08-01' LIMIT 5"))
  }
  it should "configure partition column and partition period" in {
    val actual = optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense --table=some_table " +
      "--password=secret --partition=2027-07-31 " +
      "--partitionColumn=col --partitionPeriod=P1M")

    val expected = JdbcExportArgs(
      "org.postgresql.Driver",
      "jdbc:postgresql://nonsense",
      "dbeam-extractor",
      "secret",
      "some_table",
      "dbeam_generated",
      None,
      Some("col"),
      Some(new DateTime(2027, 7, 31, 0, 0, 0, DateTimeZone.UTC)),
      Period.parse("P1M")
    )
    actual should be (expected)
    actual.buildQueries() should be (Seq("SELECT * FROM some_table " +
      "WHERE col >= '2027-07-31' AND col < '2027-08-31'"))
  }
  it should "configure avro schema namespace" in {
    val options = optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense --table=some_table " +
      "--password=secret --avroSchemaNamespace=ns")

    options should be (JdbcExportArgs(
      "org.postgresql.Driver",
      "jdbc:postgresql://nonsense",
      "dbeam-extractor",
      "secret",
      "some_table",
      "ns",
      None,
      None,
      None
    ))
  }
  it should "configure avro doc" in {
    val options = optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense --table=some_table " +
      "--password=secret --avroDoc=doc")

    options should be (JdbcExportArgs(
      "org.postgresql.Driver",
      "jdbc:postgresql://nonsense",
      "dbeam-extractor",
      "secret",
      "some_table",
      "dbeam_generated",
      avroDoc=Some("doc")
    ))
  }
}
