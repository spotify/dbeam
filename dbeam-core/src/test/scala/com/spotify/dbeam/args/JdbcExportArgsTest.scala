/*
 * Copyright 2017-2019 Spotify AB.
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

package com.spotify.dbeam.args

import java.sql.Connection
import java.util.Optional

import com.spotify.dbeam.JdbcTestFixtures
import com.spotify.dbeam.JdbcTestFixtures.recordType
import com.spotify.dbeam.options.{JdbcExportArgsFactory, JdbcExportPipelineOptions}
import org.apache.avro.file.CodecFactory
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.joda.time.{DateTime, DateTimeZone, Period}
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import slick.jdbc.H2Profile.api._

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class JdbcExportArgsTest extends FlatSpec with Matchers {

  private val connectionUrl: String =
    "jdbc:h2:mem:test;MODE=PostgreSQL;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1"
  private val db = Database.forURL(connectionUrl, driver = "org.h2.Driver")
  JdbcTestFixtures.createFixtures(db, Seq(JdbcTestFixtures.record1, JdbcTestFixtures.record2))
  private val connection: Connection = db.source.createConnection()

  private val baseQueryNoConditions = "SELECT * FROM some_table WHERE 1=1"
  private val baseUserQueryNoConditions = "SELECT * FROM (SELECT * FROM some_table) WHERE 1=1"
  private val baseQueryWithConditions = "SELECT * FROM some_table WHERE column_id > 100"
  private val baseUserQueryWithConditions = "SELECT * FROM (SELECT * FROM some_table WHERE column_id > 100) WHERE 1=1"
  private val coffeesQueryNoConditions = "SELECT * FROM coffees WHERE 1=1"
  private val coffeesQueryWithConditions = "SELECT * FROM coffees WHERE size > 10"
  private val coffeesUserQueryNoConditions = "SELECT * FROM (SELECT * FROM coffees) WHERE 1=1"
  private val coffeesUserQueryWithConditions = "SELECT * FROM (SELECT * FROM coffees WHERE size > 10) WHERE 1=1"

  def optionsFromArgs(cmdLineArgs: String): JdbcExportArgs = {
    PipelineOptionsFactory.register(classOf[JdbcExportPipelineOptions])
    val opts: PipelineOptions =
      PipelineOptionsFactory.fromArgs(cmdLineArgs.split(" "): _*).withValidation().create()
    JdbcExportArgsFactory.fromPipelineOptions(opts)
  }

  it should "fail on missing table name" in {
    a[IllegalArgumentException] should be thrownBy {
      val tableName: String = null
      QueryBuilderArgs.create(tableName)
    }
  }
  it should "fail on invalid table name" in {
    a[IllegalArgumentException] should be thrownBy {
      QueryBuilderArgs.create("*invalid#name@!")
    }
  }
  it should "create a valid SQL query from a user query" in {
    val args = QueryBuilderArgs.create("some_table", Optional.of("SELECT * FROM some_table"))
    
    args.baseSqlQuery().build should be(baseUserQueryNoConditions)
    args.tableName() should be("some_table")
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
      optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db")
    }
  }
  it should "parse correctly with missing password parameter" in {
    val options = optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db --table=some_table")

    val expected = JdbcExportArgs.create(
      JdbcAvroArgs.create(
        JdbcConnectionArgs.create("jdbc:postgresql://some_db")
          .withUsername("dbeam-extractor")
      ),
      QueryBuilderArgs.create("some_table")
    )

    options should be(expected)
  }
  it should "fail to parse invalid table parameter" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db --table=some-table-with-dash " +
        "--password=secret")
    }
  }
  it should "fail to parse non jdbc connection parameter" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=bar --table=some_table --password=secret")
    }
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=some:foo:bar --table=some_table --password=secret")
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
      optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db --table=some_table " +
        "--password=secret --partitionColumn=col")
    }
  }
  it should "fail on too old partition parameter" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db --table=some_table " +
        "--password=secret --partition=2015-01-01")
    }
  }
  it should "fail on old partition parameter with configured partition = min-partition-period" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db --table=some_table " +
        "--password=secret --partition=2015-01-01 --minPartitionPeriod=2015-01-01")
    }
  }
  it should "fail on old partition parameter with configured partition < min-partition-period" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db --table=some_table " +
        "--password=secret --partition=2015-01-01 --minPartitionPeriod=2015-01-02")
    }
  }
  it should "parse correctly for postgresql connection" in {
    val options = optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db --table=some_table " +
      "--password=secret")

    val expected = JdbcExportArgs.create(
      JdbcAvroArgs.create(
        JdbcConnectionArgs.create("jdbc:postgresql://some_db")
          .withUsername("dbeam-extractor")
          .withPassword("secret")
      ),
      QueryBuilderArgs.create("some_table")
    )

    options should be(expected)
  }
  it should "parse correctly for mysql connection" in {
    val options = optionsFromArgs("--connectionUrl=jdbc:mysql://some_db --table=some_table " +
      "--password=secret")


    val expected = JdbcExportArgs.create(
      JdbcAvroArgs.create(
        JdbcConnectionArgs.create("jdbc:mysql://some_db")
          .withUsername("dbeam-extractor")
          .withPassword("secret")
      ),
      QueryBuilderArgs.create("some_table")
    )
    options should be(expected)
  }
  it should "configure username" in {
    val options = optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db " +
      "--table=some_table --password=secret --username=some_user")

    val expected = JdbcExportArgs.create(
      JdbcAvroArgs.create(
        JdbcConnectionArgs.create("jdbc:postgresql://some_db")
          .withUsername("some_user")
          .withPassword("secret")
      ),
      QueryBuilderArgs.create("some_table")
    )

    options should be(expected)
  }
  it should "configure limit" in {
    val actual = optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db " +
      "--table=some_table --password=secret --limit=7").queryBuilderArgs()

    val expected = QueryBuilderArgs.create("some_table")
      .builder().setLimit(7L).build()
    actual should be(expected)
    buildStringQueries(actual) should
      contain theSameElementsAs Seq(s"$baseQueryNoConditions LIMIT 7")
  }
  it should "configure limit for sqlQuery" in {
    val actual = optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db " +
      "--table=some_table --sqlFile=test_query_1.sql --password=secret --limit=7").queryBuilderArgs()

    val expected = QueryBuilderArgs.create("some_table", Optional.of(baseQueryWithConditions))
      .builder().setLimit(7L).build()
    actual should be(expected)
    buildStringQueries(actual) should
      contain theSameElementsAs Seq(s"$baseUserQueryWithConditions LIMIT 7")
  }
  it should "configure partition" in {
    val actual = optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db " +
      "--table=some_table --password=secret --partition=2027-07-31").queryBuilderArgs()

    val expected = QueryBuilderArgs.create("some_table")
      .builder().setPartition(new DateTime(2027, 7, 31, 0, 0, DateTimeZone.UTC)).build()
    actual should be(expected)
    buildStringQueries(actual) should
      contain theSameElementsAs Seq(baseQueryNoConditions)
  }
  it should "configure partition with full ISO date time (Styx cron syntax)" in {
    val actual = optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db --table=some_table " +
      "--password=secret --partition=2027-07-31T13:37:59Z").queryBuilderArgs()

    val expected = QueryBuilderArgs.create("some_table")
      .builder().setPartition(new DateTime(2027, 7, 31, 13, 37, 59, DateTimeZone.UTC)).build()
    actual should be(expected)
    buildStringQueries(actual) should
      contain theSameElementsAs Seq(baseQueryNoConditions)
  }
  it should "configure partition with month date (Styx monthly schedule)" in {
    val actual = optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db " +
      "--table=some_table --password=secret --partition=2027-05").queryBuilderArgs()

    val expected = QueryBuilderArgs.create("some_table")
      .builder().setPartition(new DateTime(2027, 5, 1, 0, 0, 0, DateTimeZone.UTC)).build()
    actual should be(expected)
    buildStringQueries(actual) should
      contain theSameElementsAs Seq(baseQueryNoConditions)
  }
  it should "configure partition column" in {
    val actual = optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db --table=some_table " +
      "--password=secret --partition=2027-07-31 --partitionColumn=col").queryBuilderArgs()

    val expected = QueryBuilderArgs.create("some_table")
      .builder()
      .setPartitionColumn("col")
      .setPartition(new DateTime(2027, 7, 31, 0, 0, 0, DateTimeZone.UTC)).build()
    actual should be(expected)
    buildStringQueries(actual) should
      contain theSameElementsAs Seq(s"$baseQueryNoConditions " +
      "AND col >= '2027-07-31' AND col < '2027-08-01'")
  }
  
  it should "configure partition column for sqlQuery" in {
    val actual = optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db --table=some_table " +
      "--password=secret --sqlFile=test_query_1.sql --partition=2027-07-31 --partitionColumn=col").queryBuilderArgs()

    val expected = QueryBuilderArgs.create("some_table", Optional.of(baseQueryWithConditions))
      .builder()
      .setPartitionColumn("col")
      .setPartition(new DateTime(2027, 7, 31, 0, 0, 0, DateTimeZone.UTC)).build()
    actual should be(expected)
    buildStringQueries(actual) should
      contain theSameElementsAs Seq(s"$baseUserQueryWithConditions" +
      " AND col >= '2027-07-31' AND col < '2027-08-01'")
  }

  private def buildStringQueries(actual: QueryBuilderArgs) = {
    actual.buildQueries(connection).asScala.toList
  }

  it should "configure partition column and limit" in {
    val actual = optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db --table=some_table " +
      "--password=secret --partition=2027-07-31 --partitionColumn=col --limit=5").queryBuilderArgs()

    val expected = QueryBuilderArgs.create("some_table")
      .builder()
      .setLimit(5L)
      .setPartitionColumn("col")
      .setPartition(new DateTime(2027, 7, 31, 0, 0, 0, DateTimeZone.UTC)).build()
    actual should be(expected)

    buildStringQueries(actual) should
      contain theSameElementsAs Seq(
      s"$baseQueryNoConditions AND col >= '2027-07-31' AND col < '2027-08-01' LIMIT 5")
  }
  it should "configure partition column and limit for sqlQuery" in {
    val actual = optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db --table=some_table " +
      "--password=secret --sqlFile=test_query_1.sql --partition=2027-07-31 --partitionColumn=col --limit=5").queryBuilderArgs()

    val expected = QueryBuilderArgs.create("some_table", Optional.of(baseQueryWithConditions))
      .builder()
      .setLimit(5L)
      .setPartitionColumn("col")
      .setPartition(new DateTime(2027, 7, 31, 0, 0, 0, DateTimeZone.UTC)).build()
    actual should be(expected)

    buildStringQueries(actual) should
      contain theSameElementsAs Seq(
      s"$baseUserQueryWithConditions AND col >= '2027-07-31' AND col < '2027-08-01' LIMIT 5")
  }
  it should "configure partition column and partition period" in {
    val actual = optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db --table=some_table " +
      "--password=secret --partition=2027-07-31 " +
      "--partitionColumn=col --partitionPeriod=P1M").queryBuilderArgs()
    val expected = QueryBuilderArgs.create("some_table")
      .builder()
      .setPartitionColumn("col")
      .setPartitionPeriod(Period.parse("P1M"))
      .setPartition(new DateTime(2027, 7, 31, 0, 0, 0, DateTimeZone.UTC)).build()
    actual should be(expected)
    buildStringQueries(actual) should
      contain theSameElementsAs Seq(
      s"$baseQueryNoConditions " +
        "AND col >= '2027-07-31' AND col < '2027-08-31'")
  }

  it should "create queries for split column of integer type" in {
    val actual = optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db --table=COFFEES " +
      "--password=secret --splitColumn=ROWNUM --queryParallelism=5").queryBuilderArgs()
    val baseCoffeesQueryNoConditions = "SELECT * FROM COFFEES WHERE 1=1"
    val expected = QueryBuilderArgs.create("COFFEES")
      .builder()
      .setSplitColumn("ROWNUM")
      .setQueryParallelism(5) // We have only two values of ROWNUM but still give a higher parallism
      .build()
    actual should be(expected)
    buildStringQueries(actual) should
      contain theSameElementsAs List(
      s"$baseCoffeesQueryNoConditions " +
        "AND ROWNUM >= 1 AND ROWNUM <= 2")
  }

  it should "create queries for split column of integer type for sqlQuery" in {
    val actual = optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db --table=COFFEES " +
      "--password=secret --sqlFile=coffees_query_1.sql --splitColumn=ROWNUM --queryParallelism=5")
      .queryBuilderArgs()
    val expected = QueryBuilderArgs.create("COFFEES", Optional.of(coffeesQueryWithConditions))
      .builder()
      .setSplitColumn("ROWNUM")
      .setQueryParallelism(5) // We have only two values of ROWNUM but still give a higher parallism
      .build()
    actual should be(expected)
    val msgs = buildStringQueries(actual)
    msgs should
      contain theSameElementsAs Seq(
      s"$coffeesUserQueryWithConditions" +
        " AND ROWNUM >= 1 AND ROWNUM <= 2")
  }

  /**
    * Creates a sequence of n records using record as a clone source.
    * 
    * @param n
    * @param record
    * @return sequence of records.
    */
  def createRecordSeq(n: Int, record: recordType): Seq[recordType] = {
    (1 to n).map(x => record.copy(_1 = record._1 + x, _13 = record._13 + x))
  }
  

  it should "create queries for split column of integer type for sqlQuery in many splits" in {
    //set-up fixture
    val parallelism = 5
    val numberOfRecords = 25 
    val moreRecords = createRecordSeq(numberOfRecords-2, JdbcTestFixtures.record2);
    
    JdbcTestFixtures.createFixtures(db, Seq(JdbcTestFixtures.record1, JdbcTestFixtures.record2) ++ moreRecords)
    
    val actual = optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db --table=COFFEES " +
      "--password=secret --sqlFile=coffees_query_1.sql --splitColumn=ROWNUM --queryParallelism=5")
      .queryBuilderArgs()
    val expected = QueryBuilderArgs.create("COFFEES", Optional.of(coffeesQueryWithConditions))
      .builder()
      .setSplitColumn("ROWNUM")
      .setQueryParallelism(parallelism)
      .build()
    actual should be(expected)
    val msgs = buildStringQueries(actual)
    msgs should
      contain theSameElementsAs Seq(
      s"$coffeesUserQueryWithConditions" + " AND ROWNUM >= 1 AND ROWNUM < 6",
      s"$coffeesUserQueryWithConditions" + " AND ROWNUM >= 6 AND ROWNUM < 11",
      s"$coffeesUserQueryWithConditions" + " AND ROWNUM >= 11 AND ROWNUM < 16",
      s"$coffeesUserQueryWithConditions" + " AND ROWNUM >= 16 AND ROWNUM < 21",
      s"$coffeesUserQueryWithConditions" + " AND ROWNUM >= 21 AND ROWNUM <= 25"
    )
  }

  it should "create queries with partition column and split column with queryParallelism" in {
    val actual = optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db --table=coffees " +
      "--password=secret --partitionColumn=created --partition=2027-07-31 --partitionPeriod=P1M " +
      "--splitColumn=ROWNUM --queryParallelism=5").queryBuilderArgs()
      
    val expected = QueryBuilderArgs.create("coffees")
      .builder()
      .setPartitionColumn("created")
      .setPartitionPeriod(Period.parse("P1M"))
      .setPartition(new DateTime(2027, 7, 31, 0, 0, 0, DateTimeZone.UTC))
      .setSplitColumn("ROWNUM")
      .setQueryParallelism(5) // We have only two values of ROWNUM but still give a higher parallism
      .build()
    actual should be(expected)
    buildStringQueries(actual) should
      contain theSameElementsAs Seq(
      s"$coffeesQueryNoConditions" +
        " AND created >= '2027-07-31' AND created < '2027-08-31'" +
        " AND ROWNUM >= 0 AND ROWNUM <= 0")
  }

  it should "configure avro schema namespace" in {
    val options = optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db --table=some_table " +
      "--password=secret --avroSchemaNamespace=ns")

    options.avroSchemaNamespace() should be("ns")
  }
  it should "configure avro doc" in {
    val options = optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db --table=some_table " +
      "--password=secret --avroDoc=doc")

    options.avroDoc() should be(Optional.of("doc"))
  }
  it should "configure use avro logical types" in {
    val options = optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db --table=some_table " +
      "--password=secret --useAvroLogicalTypes=true")

    options.useAvroLogicalTypes() shouldBe true
  }
  it should "configure fetch size" in {
    val options = optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db --table=some_table " +
      "--password=secret --fetchSize=1234")

    options.jdbcAvroOptions().fetchSize() should be(1234)
  }
  it should "configure deflate compression level on avro codec" in {
    val options = optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db --table=some_table " +
      "--password=secret --avroCodec=deflate7")

    options.jdbcAvroOptions().avroCodec() should be("deflate7")
    options.jdbcAvroOptions().getCodecFactory.toString should
      be(CodecFactory.deflateCodec(7).toString)
  }
  it should "configure snappy as avro codec" in {
    val options = optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db --table=some_table " +
      "--password=secret --avroCodec=snappy")

    options.jdbcAvroOptions().avroCodec() should be("snappy")
    options.jdbcAvroOptions().getCodecFactory.toString should
      be(CodecFactory.snappyCodec().toString)
  }
  it should "fail on invalid avro codec" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db " +
        "--table=some_table --password=secret --avroCodec=lzma")
    }
  }

  it should "fail on queryParallelism with no split column" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db " +
        "--table=some_table --password=secret --queryParallelism=10")
        .queryBuilderArgs().buildQueries(connection)
    }
  }

  it should "fail on split column is specified with no queryParallelism" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db " +
        "--table=some_table --password=secret --splitColumn=id")
        .queryBuilderArgs().buildQueries(connection)
    }
  }

  it should "not accept 0 queryParallelism" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db " +
        "--table=some_table --password=secret --queryParallelism=0 --splitColumn=id")
        .queryBuilderArgs().buildQueries(connection)
    }
  }

  it should "not accept -ve queryParallelism" in {
    a[IllegalArgumentException] should be thrownBy {
      optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db " +
        "--table=some_table --password=secret --queryParallelism=-5 --splitColumn=id")
        .queryBuilderArgs().buildQueries(connection)
    }
  }
}
