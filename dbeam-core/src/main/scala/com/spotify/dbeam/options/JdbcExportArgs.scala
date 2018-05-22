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

import java.util.concurrent.ThreadLocalRandom

import com.spotify.scio._
import org.apache.beam.sdk.options.{ApplicationNameOptions, PipelineOptions, PipelineOptionsFactory}
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, Days, Period, ReadablePeriod}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

case class JdbcExportArgs(driverClass: String,
                          connectionUrl: String,
                          username: String,
                          password: String,
                          tableName: String,
                          avroSchemaNamespace: String,
                          limit: Option[Int] = None,
                          partitionColumn: Option[String] = None,
                          partition: Option[DateTime] = None,
                          partitionPeriod: ReadablePeriod = Days.ONE,
                          avroDoc: Option[String] = None,
                          useAvroLogicalTypes: Boolean = false)
  extends JdbcConnectionArgs with QueryArgs {

  require(checkTableName(), s"Invalid SQL table name: $tableName")
  require(partitionColumn.isEmpty || partition.isDefined,
    "To use --partitionColumn the --partition parameter must also be configured")

}

object JdbcExportArgs {
  val log: Logger = LoggerFactory.getLogger(JdbcExportArgs.getClass)

  private def validatePartition(partitionDateTime: DateTime, minPartitionDateTime: DateTime)
  : DateTime = {
    require(partitionDateTime.isAfter(minPartitionDateTime),
      "Too old partition date %s. Use a partition date >= %s or use --skip-partition-check".format(
        partitionDateTime, minPartitionDateTime
      ))
    partitionDateTime
  }

  private def parseDateTime(input: String): DateTime =
    DateTime.parse(input.stripSuffix("Z"), ISODateTimeFormat.localDateOptionalTimeParser)

  def fromPipelineOptions(options: PipelineOptions): JdbcExportArgs = {
    val exportOptions: JdbcExportPipelineOptions = options.as(classOf[JdbcExportPipelineOptions])
    val partitionPeriod: ReadablePeriod = Option(exportOptions.getPartitionPeriod)
      .map(Period.parse).getOrElse(Days.ONE)
    val partitionColumn: Option[String] = Option(exportOptions.getPartitionColumn)
    val skipPartitionCheck: Boolean = exportOptions.isSkipPartitionCheck
    val partition: Option[DateTime] = Option(exportOptions.getPartition).map(parseDateTime)

    require(exportOptions.getConnectionUrl != null, "'connectionUrl' must be defined")
    require(exportOptions.getTable != null, "'table' must be defined")

    if (!skipPartitionCheck && partitionColumn.isEmpty) {
      val minPartitionDateTime = Option(exportOptions.getMinPartitionPeriod)
        .map(parseDateTime)
        .getOrElse(DateTime.now().minus(partitionPeriod.toPeriod.multipliedBy(2)))
      partition.map(validatePartition(_, minPartitionDateTime))
    }

    JdbcExportArgs(
      JdbcConnectionUtil.getDriverClass(exportOptions.getConnectionUrl),
      exportOptions.getConnectionUrl,
      exportOptions.getUsername,
      PipelineOptionsUtil.readPassword(exportOptions).orNull,
      exportOptions.getTable,
      exportOptions.getAvroSchemaNamespace,
      Option(exportOptions.getLimit).map(_.toInt),
      partitionColumn,
      partition,
      partitionPeriod,
      Option(exportOptions.getAvroDoc),
      exportOptions.isUseAvroLogicalTypes
    )
  }

  def fromPipelineOptionsConfigured(options: PipelineOptions): JdbcExportArgs = {
    val args = fromPipelineOptions(options)
    Try(options.as(classOf[ApplicationNameOptions])).foreach(_.setAppName("JdbcAvroJob"))
    if (options.getJobName == null) {
      val dbName = args.createConnection().getCatalog.toLowerCase().replaceAll("[^a-z0-9]", "")
      val tableName = args.tableName.toLowerCase().replaceAll("[^a-z0-9]", "")
      val randomPart = Integer.toHexString(ThreadLocalRandom.current().nextInt())
      options.setJobName(s"dbeam-${dbName}-${tableName}-${randomPart}")
    }
    args
  }

  def contextAndArgs(cmdlineArgs: Array[String]): (ScioContext, JdbcExportArgs, String) = {
    PipelineOptionsFactory.register(classOf[JdbcExportPipelineOptions])
    PipelineOptionsFactory.register(classOf[OutputOptions])
    val opts = PipelineOptionsFactory.fromArgs(cmdlineArgs: _*).withValidation().create()
    (ScioContext(opts),
      JdbcExportArgs.fromPipelineOptionsConfigured(opts),
      opts.as(classOf[OutputOptions]).getOutput)
  }

}
