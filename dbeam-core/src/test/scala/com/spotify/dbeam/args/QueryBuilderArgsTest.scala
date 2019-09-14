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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.sql.Connection
import java.util.Comparator

import com.spotify.dbeam.options.{JdbcExportArgsFactory, JdbcExportPipelineOptions}
import com.spotify.dbeam.{JdbcTestFixtures, TestHelper}
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.joda.time.{DateTime, DateTimeZone, Period}
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import slick.jdbc.H2Profile.api._

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class QueryBuilderArgsTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val connectionUrl: String =
    "jdbc:h2:mem:test;MODE=PostgreSQL;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1"
  private val db = Database.forURL(connectionUrl, driver = "org.h2.Driver")
  JdbcTestFixtures.createFixtures(db)
  private val connection: Connection = db.source.createConnection()

  private val baseQueryNoConditions = "SELECT * FROM some_table WHERE 1=1"
  private val baseUserQueryNoConditions = "SELECT * FROM (SELECT * FROM some_table) WHERE 1=1"
  private val baseQueryWithConditions = "SELECT * FROM some_table WHERE column_id > 100"
  private val baseUserQueryWithConditions = "SELECT * FROM (SELECT * FROM some_table WHERE column_id > 100) WHERE 1=1"
  private val coffeesQueryNoConditions = "SELECT * FROM COFFEES WHERE 1=1"
  private val coffeesQueryWithConditions = "SELECT * FROM COFFEES WHERE SIZE > 10"
  private val coffeesUserQueryNoConditions = "SELECT * FROM (SELECT * FROM COFFEES) WHERE 1=1"
  private val coffeesUserQueryWithConditions = "SELECT * FROM (SELECT * FROM COFFEES WHERE SIZE > 10) WHERE 1=1"
  
  private val tempFilesDir = TestHelper.createTmpDirName("jdbc-export-args-test")
  private val testSqlFilePath: Path = Paths.get(tempFilesDir.toString, "test_query_1.sql")
  private val coffeesSqlFilePath = Paths.get(tempFilesDir.toString, "coffees_query_1.sql")
  private val testSqlFile = testSqlFilePath.toString
  private val coffeesSqlFile = coffeesSqlFilePath.toString

  override def beforeAll(): Unit = {
    Files.createDirectories(tempFilesDir)
    Files.createFile(testSqlFilePath)
    Files.createFile(coffeesSqlFilePath)
    Files.write(testSqlFilePath,
      "SELECT * FROM some_table WHERE column_id > 100".getBytes(StandardCharsets.UTF_8))
    Files.write(coffeesSqlFilePath,
      "SELECT * FROM COFFEES WHERE SIZE > 10".getBytes(StandardCharsets.UTF_8))
  }

  override protected def afterAll(): Unit = {
    Files.delete(testSqlFilePath)
    Files.delete(coffeesSqlFilePath)
    Files.walk(tempFilesDir)
      .sorted(Comparator.reverseOrder())
      .iterator()
      .asScala
      .foreach((p: Path) => Files.delete(p))
  }

  def optionsFromArgs(cmdLineArgs: String): JdbcExportArgs = {
    PipelineOptionsFactory.register(classOf[JdbcExportPipelineOptions])
    val opts: PipelineOptions =
      PipelineOptionsFactory.fromArgs(cmdLineArgs.split(" "): _*).withValidation().create()
    JdbcExportArgsFactory.fromPipelineOptions(opts)
  }

  private def buildStringQueries(actual: QueryBuilderArgs) = {
    actual.buildQueries(connection).asScala.toList
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
    val args = QueryBuilderArgs.create("some_table", "SELECT * FROM some_table")
    
    args.baseSqlQuery().build should be(baseUserQueryNoConditions)
    args.tableName() should be("some_table")
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
      "--table=some_table --sqlFile=%s --password=secret --limit=7".format(testSqlFile)).queryBuilderArgs()

    val expected = QueryBuilderArgs.create("some_table", baseQueryWithConditions)
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
    val actual = optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db "
      + "--table=some_table --password=secret --partition=2027-05").queryBuilderArgs()

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
      "--password=secret --sqlFile=%s --partition=2027-07-31 --partitionColumn=col".format(testSqlFile))
      .queryBuilderArgs()

    val expected = QueryBuilderArgs.create("some_table", baseQueryWithConditions)
      .builder()
      .setPartitionColumn("col")
      .setPartition(new DateTime(2027, 7, 31, 0, 0, 0, DateTimeZone.UTC)).build()
    actual should be(expected)
    buildStringQueries(actual) should
      contain theSameElementsAs Seq(s"$baseUserQueryWithConditions" +
      " AND col >= '2027-07-31' AND col < '2027-08-01'")
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
      "--password=secret --sqlFile=%s --partition=2027-07-31 --partitionColumn=col --limit=5".format(testSqlFile))
      .queryBuilderArgs()

    val expected = QueryBuilderArgs.create("some_table", baseQueryWithConditions)
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
      "--password=secret --sqlFile=%s --splitColumn=ROWNUM --queryParallelism=5".format(coffeesSqlFile))
      .queryBuilderArgs()
    val expected = QueryBuilderArgs.create("COFFEES", coffeesQueryWithConditions)
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

  it should "create queries for split column of integer type for sqlQuery in many splits" in {
    val actual = optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db --table=COFFEES " +
      "--password=secret --sqlFile=%s --splitColumn=ROWNUM --queryParallelism=2".format(coffeesSqlFile))
      .queryBuilderArgs()
    val expected = QueryBuilderArgs.create("COFFEES", coffeesQueryWithConditions)
      .builder()
      .setSplitColumn("ROWNUM")
      .setQueryParallelism(2)
      .build()
    actual should be(expected)
    val msgs = buildStringQueries(actual)
    msgs should
      contain theSameElementsAs Seq(
      s"$coffeesUserQueryWithConditions" + " AND ROWNUM >= 1 AND ROWNUM <= 2"
    )
  }

  it should "create queries with partition column and split column with queryParallelism" in {
    val actual = optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db --table=COFFEES " +
      "--password=secret --partitionColumn=CREATED --partition=2027-07-31 --partitionPeriod=P1M " +
      "--splitColumn=ROWNUM --queryParallelism=5").queryBuilderArgs()
      
    val expected = QueryBuilderArgs.create("COFFEES")
      .builder()
      .setPartitionColumn("CREATED")
      .setPartitionPeriod(Period.parse("P1M"))
      .setPartition(new DateTime(2027, 7, 31, 0, 0, 0, DateTimeZone.UTC))
      .setSplitColumn("ROWNUM")
      .setQueryParallelism(5) // We have only two values of ROWNUM but still give a higher parallism
      .build()
    actual should be(expected)
    buildStringQueries(actual) should
      contain theSameElementsAs Seq(
      s"$coffeesQueryNoConditions" +
        " AND CREATED >= '2027-07-31' AND CREATED < '2027-08-31'" +
        " AND ROWNUM >= 0 AND ROWNUM <= 0")
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
