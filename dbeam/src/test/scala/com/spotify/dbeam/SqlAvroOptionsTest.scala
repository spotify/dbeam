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

import com.spotify.scio.{Args, ScioContext}
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.joda.time.{DateTime, DateTimeZone, Period}
import org.scalatest._

class SqlAvroOptionsTest extends FlatSpec with Matchers {

  def optionsFromArgs(cmdLineArgs: String): SqlAvroOptions = {
    PipelineOptionsFactory.register(classOf[SqlReadOptions])
    val (opts: PipelineOptions, args: Args) =
      ScioContext.parseArguments[PipelineOptions](cmdLineArgs.split(" "))
    SqlAvroOptions.fromArgsAndOptions(opts, args)
  }

  it should "fail parse invalid arguments" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("")
      optionsFromArgs("--foo=bar")
    }
  }
  it should "fail to parse with missing connectionUrl parameter" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--table=some-table --output=/path")
    }
  }
  it should "fail to parse with missing table parameter" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense --output=/path")
    }
  }
  it should "fail to parse with missing output parameter" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense --table=some_table")
    }
  }
  it should "fail to parse with missing password parameter" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense --table=some_table " +
        "--output=/path")
    }
  }
  it should "fail to parse invalid table parameter" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense --table=some-table " +
        "--output=/path --password=secret")
    }
  }
  it should "fail to parse non jdbc connection parameter" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=bar --table=some_table --output=/path --password=secret")
    }
  }
  it should "fail to parse unsupported jdbc connection parameter" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=jdbc:paradox:./foo --table=some_table " +
        "--output=/path --password=secret")
    }
  }
  it should "fail to parse missing partition parameter but partition column present" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense --table=some_table " +
        "--output=/path --password=secret --partitionColumn=col")
    }
  }
  it should "fail on too old partition parameter" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense --table=some_table " +
        "--output=/path --password=secret --partition=2015-01-01")
    }
  }
  it should "fail on old partition parameter with configured partition = min-partition-period" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense --table=some_table " +
        "--output=/path --password=secret --partition=2015-01-01 --minPartitionPeriod=2015-01-01")
    }
  }
  it should "fail on old partition parameter with configured partition < min-partition-period" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense --table=some_table " +
        "--output=/path --password=secret --partition=2015-01-01 --minPartitionPeriod=2015-01-02")
    }
  }
  it should "parse correctly for postgresql connection" in {
    val options = optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense --table=some_table " +
      "--output=/path --password=secret")

    options should be (SqlAvroOptions(
      "org.postgresql.Driver",
      "jdbc:postgresql://nonsense",
      "dbeam-extractor",
      "secret",
      "some_table",
      "/path",
      "dbeam_generated",
      None,
      None,
      None
    ))
  }
  it should "parse correctly for mysql connection" in {
    val options = optionsFromArgs("--connectionUrl=jdbc:mysql://nonsense --table=some_table " +
      "--output=/path --password=secret")

    options should be (SqlAvroOptions(
      "com.mysql.jdbc.Driver",
      "jdbc:mysql://nonsense",
      "dbeam-extractor",
      "secret",
      "some_table",
      "/path",
      "dbeam_generated",
      None,
      None,
      None
    ))
  }
  it should "configure username" in {
    val options = optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense " +
      "--table=some_table --output=/path --password=secret --username=some_user")

    options should be (SqlAvroOptions(
      "org.postgresql.Driver",
      "jdbc:postgresql://nonsense",
      "some_user",
      "secret",
      "some_table",
      "/path",
      "dbeam_generated",
      None,
      None,
      None
    ))
  }
  it should "configure limit" in {
    val actual = optionsFromArgs("--connectionUrl=jdbc:postgresql://nonsense " +
      "--table=some_table --output=/path --password=secret --limit=7")

    val expected = SqlAvroOptions(
      "org.postgresql.Driver",
      "jdbc:postgresql://nonsense",
      "dbeam-extractor",
      "secret",
      "some_table",
      "/path",
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
      "--table=some_table --output=/path --password=secret --partition=2027-07-31")

    val expected = SqlAvroOptions(
      "org.postgresql.Driver",
      "jdbc:postgresql://nonsense",
      "dbeam-extractor",
      "secret",
      "some_table",
      "/path",
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
      "--output=/path --password=secret --partition=2027-07-31T13:37:59Z")

    val expected = SqlAvroOptions(
      "org.postgresql.Driver",
      "jdbc:postgresql://nonsense",
      "dbeam-extractor",
      "secret",
      "some_table",
      "/path",
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
      "--table=some_table --output=/path --password=secret --partition=2027-05")

    val expected = SqlAvroOptions(
      "org.postgresql.Driver",
      "jdbc:postgresql://nonsense",
      "dbeam-extractor",
      "secret",
      "some_table",
      "/path",
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
      "--output=/path --password=secret --partition=2027-07-31 --partitionColumn=col")

    val expected = SqlAvroOptions(
      "org.postgresql.Driver",
      "jdbc:postgresql://nonsense",
      "dbeam-extractor",
      "secret",
      "some_table",
      "/path",
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
      "--output=/path --password=secret --partition=2027-07-31 --partitionColumn=col --limit=5")

    val expected = SqlAvroOptions(
      "org.postgresql.Driver",
      "jdbc:postgresql://nonsense",
      "dbeam-extractor",
      "secret",
      "some_table",
      "/path",
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
      "--output=/path --password=secret --partition=2027-07-31 " +
      "--partitionColumn=col --partitionPeriod=P1M")

    val expected = SqlAvroOptions(
      "org.postgresql.Driver",
      "jdbc:postgresql://nonsense",
      "dbeam-extractor",
      "secret",
      "some_table",
      "/path",
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
      "--output=/path --password=secret --avroSchemaNamespace=ns")

    options should be (SqlAvroOptions(
      "org.postgresql.Driver",
      "jdbc:postgresql://nonsense",
      "dbeam-extractor",
      "secret",
      "some_table",
      "/path",
      "ns",
      None,
      None,
      None
    ))
  }
}
