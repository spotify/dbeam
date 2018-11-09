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

import java.sql.Connection

import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

case class JdbcExportArgs(driverClass: String,
                          connectionUrl: String,
                          username: String,
                          password: String,
                          queryBuilderArgs: QueryBuilderArgs,
                          avroSchemaNamespace: String = "dbeam_generated",
                          avroDoc: Option[String] = None,
                          useAvroLogicalTypes: Boolean = false,
                          fetchSize: Int = 10000,
                          avroCodec: String = "deflate6") {

  val jdbcAvroOptions: JdbcAvroOptions = JdbcAvroOptions.create(
    JdbcConnectionConfiguration.create(driverClass, connectionUrl)
      .withUsername(username)
      .withPassword(password),
    fetchSize,
    avroCodec)

  def buildQueries(): java.lang.Iterable[String] = queryBuilderArgs.buildQueries()

  def createConnection(): Connection =
    jdbcAvroOptions.jdbcConnectionConfiguration.createConnection

}

object JdbcExportArgs {
  val log: Logger = LoggerFactory.getLogger(JdbcExportArgs.getClass)

  def fromPipelineOptions(options: PipelineOptions): JdbcExportArgs = {
    val exportOptions: JdbcExportPipelineOptions = options.as(classOf[JdbcExportPipelineOptions])
    require(exportOptions.getConnectionUrl != null, "'connectionUrl' must be defined")

    val jdbcAvroOptions: JdbcAvroOptions = JdbcAvroOptions.create(
      JdbcConnectionConfiguration.create(
        JdbcConnectionUtil.getDriverClass(exportOptions.getConnectionUrl),
        exportOptions.getConnectionUrl)
        .withUsername(exportOptions.getUsername)
        .withPassword(PasswordReader.readPassword(exportOptions).orElse(null)),
      exportOptions.getFetchSize,
      exportOptions.getAvroCodec)

    JdbcExportArgs(
      JdbcConnectionUtil.getDriverClass(exportOptions.getConnectionUrl),
      exportOptions.getConnectionUrl,
      exportOptions.getUsername,
      PasswordReader.readPassword(exportOptions).orElse(null),
      QueryBuilderArgs.create(exportOptions),
      exportOptions.getAvroSchemaNamespace,
      Option(exportOptions.getAvroDoc),
      exportOptions.isUseAvroLogicalTypes,
      exportOptions.getFetchSize,
      exportOptions.getAvroCodec
    )
  }

  def parseOptions(cmdlineArgs: Array[String]): (PipelineOptions, JdbcExportArgs, String) = {
    PipelineOptionsFactory.register(classOf[JdbcExportPipelineOptions])
    PipelineOptionsFactory.register(classOf[OutputOptions])
    val opts = PipelineOptionsFactory.fromArgs(cmdlineArgs: _*).withValidation().create()
    (opts,
      JdbcExportArgs.fromPipelineOptions(opts),
      opts.as(classOf[OutputOptions]).getOutput)
  }

}
