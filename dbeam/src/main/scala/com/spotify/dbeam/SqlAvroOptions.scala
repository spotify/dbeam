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

import java.sql.{Connection, DriverManager}

import com.spotify.scio._
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, Days, Period, ReadablePeriod}
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source
import scala.util.matching.Regex

trait JdbcConnectionOptions {
  def driverClass: String
  def connectionUrl: String
  def username: String
  def password: String

  def createConnection(): Connection = {
    Class.forName(driverClass)
    DriverManager.getConnection(connectionUrl, username, password)
  }
}

trait QueryOptions {
  def tableName: String
  def limit: Option[Int]
  def partitionColumn: Option[String]
  def partition: Option[DateTime]
  def partitionPeriod: ReadablePeriod

  def buildQueries(): Iterable[String] = {
    val limit = this.limit.map(" LIMIT %d".format(_)).getOrElse("")
    val where = (partition, partitionColumn) match {
      case (Some(partition), Some(partitionColumn)) => {
        val datePartition = partition.toLocalDate
        val nextPartition = datePartition.plus(partitionPeriod).toString
        s" WHERE ${partitionColumn} >= '${datePartition}'" +
        s" AND ${partitionColumn} < '${nextPartition}'"
      }
      case _ => ""
    }
    Seq(s"SELECT * FROM ${tableName}${where}${limit}")
  }

}

case class SqlAvroOptions(driverClass: String,
                          connectionUrl: String,
                          username: String,
                          password: String,
                          tableName: String,
                          output: String,
                          avroSchemaNamespace: String,
                          limit: Option[Int] = None,
                          partitionColumn: Option[String] = None,
                          partition: Option[DateTime] = None,
                          partitionPeriod: ReadablePeriod = Days.ONE)
  extends JdbcConnectionOptions with QueryOptions {
  private val validTableName: Regex = "^[a-zA-Z_][a-zA-Z0-9_]*$".r

  require(validTableName.pattern.matcher(tableName).matches,
    s"Invalid SQL table name: ${tableName}" )
  require(partitionColumn.isEmpty || partition.isDefined,
    "To use --partitionColumn the --partition parameter must also be configured")

  def pathInOutput(subPath: String): String = output.replaceAll("\\/+$", "") + subPath
}

object SqlAvroOptions {
  val log: Logger = LoggerFactory.getLogger(SqlAvroOptions.getClass)

  private def validatePartition(partitionDateTime: DateTime, minPartitionDateTime: DateTime)
  : Unit = {
    require(partitionDateTime.isAfter(minPartitionDateTime),
      "Too old partition date %s. Use a partition date >= %s or use --skip-partition-check".format(
        partitionDateTime, minPartitionDateTime
      ))
  }

  private val driverMapping = Map(
    "postgresql" -> "org.postgresql.Driver",
    "mysql" -> "com.mysql.jdbc.Driver",
    "h2" -> "org.h2.Driver"
  )

  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  private def getDriverClass(url: String): String = {
    log.debug("Inferring database type from connection URL: " + url)

    val parts: Array[String] = url.split(":", 3)
    require(parts(0) == "jdbc",
      s"Invalid jdbc connection URL: ${url}. Expect jdbc:postgresql or jdbc:mysql as prefix.")
    val mappedClass: Option[String] = driverMapping.get(parts(1))
      .map(Class.forName(_).getCanonicalName)
    require(mappedClass.isDefined,
      s"Invalid jdbc connection URL: ${url}. Expect jdbc:postgresql or jdbc:mysql as prefix.")
    log.debug("Using database driver class with name: " + mappedClass.get)
    mappedClass.get
  }

  private def parseDateTime(input: String): DateTime =
    DateTime.parse(input.stripSuffix("Z"), ISODateTimeFormat.localDateOptionalTimeParser)

  def fromArgsAndOptions(options: PipelineOptions, args: Args): SqlAvroOptions = {
    val sqlReadOptions = options.as(classOf[SqlReadOptions])
    val partitionPeriod: ReadablePeriod = Option(sqlReadOptions.getPartitionPeriod)
      .map(Period.parse(_))
      .getOrElse(Days.ONE)
    val partitionColumn: Option[String] = Option(sqlReadOptions.getPartitionColumn)
    val skipPartitionCheck: Boolean = sqlReadOptions.isSkipPartitionCheck
    val partition: Option[DateTime] = Option(sqlReadOptions.getPartition).map(parseDateTime)

    val password: Option[String] =  Option(sqlReadOptions.getPasswordFile)
      .map(Source.fromFile(_).mkString.stripLineEnd)
      .orElse(args.optional("password"))

    require(sqlReadOptions.getConnectionUrl != null, "'connectionUrl' must be defined")
    require(sqlReadOptions.getOutput != null, "'output' must be defined")
    require(sqlReadOptions.getTable != null, "'table' must be defined")

    if (!skipPartitionCheck && partitionColumn.isEmpty) {
      val minPartitionDateTime = args.optional("minPartitionPeriod")
        .map(parseDateTime)
        .getOrElse(DateTime.now().minus(partitionPeriod.toPeriod().multipliedBy(2)))
      partition.map(validatePartition(_, minPartitionDateTime))
    }

    SqlAvroOptions(
      getDriverClass(sqlReadOptions.getConnectionUrl),
      sqlReadOptions.getConnectionUrl,
      sqlReadOptions.getUsername,
      password.orNull,
      sqlReadOptions.getTable,
      sqlReadOptions.getOutput,
      sqlReadOptions.getAvroSchemaNamespace,
      Option(sqlReadOptions.getLimit).map(_.toInt),
      partitionColumn,
      partition,
      partitionPeriod
    )
  }

  def contextAndOptions(cmdlineArgs: Array[String]): (ScioContext, SqlAvroOptions) = {
    PipelineOptionsFactory.register(classOf[SqlReadOptions])
    val (sc: ScioContext, args: Args) = ContextAndArgs(cmdlineArgs)
    val options: SqlAvroOptions = SqlAvroOptions.fromArgsAndOptions(sc.options, args)
    (sc, options)
  }
}
