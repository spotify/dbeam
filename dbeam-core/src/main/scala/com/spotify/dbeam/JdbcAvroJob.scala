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

import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.sql.{Connection, ResultSet}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.spotify.dbeam.options.{JdbcConnectionArgs, JdbcExportArgs}
import com.spotify.scio._
import com.spotify.scio.metrics.MetricValue
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.metrics.MetricName
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.util.MimeTypes
import org.apache.beam.sdk.values.{PCollection, PDone}
import org.slf4j.{Logger, LoggerFactory}

object JdbcAvroJob {
  val log: Logger = LoggerFactory.getLogger(JdbcAvroJob.getClass)

  /**
    * Generate Avro schema by reading one row
    * Also save schema to output target and expose time to generate schema as a Beam counter
    */
  def createSchema(sc: ScioContext, args: JdbcExportArgs): Schema = {
    val startTimeMillis: Long = System.currentTimeMillis()
    val connection: Connection = args.createConnection()
    val avroDoc = args.avroDoc.getOrElse(s"Generate schema from JDBC ResultSet from " +
      s"${args.tableName} ${connection.getMetaData.getURL}")
    val generatedSchema: Schema = JdbcAvroConversions.createSchemaByReadingOneRow(
      connection, args.tableName, args.avroSchemaNamespace, avroDoc, args.useAvroLogicalTypes)
    val elapsedTimeSchema: Long = System.currentTimeMillis() - startTimeMillis
    log.info(s"Elapsed time to schema ${elapsedTimeSchema / 1000.0} seconds")
    sc
      .parallelize(Seq(0))
      .withName("ExposeSchemaCounters")
      .map(inp => {
        ScioMetrics.counter("schemaElapsedTimeMs").inc(elapsedTimeSchema)
        inp
      })
    generatedSchema
  }

  /**
    * Creates Beam transform to read data from JDBC and save to Avro, in a single step
    */
  def jdbcAvroTransform(output: String,
                        options: JdbcConnectionArgs,
                        generatedSchema: Schema): PTransform[PCollection[String], PDone] = {
    class ResultSetGenericRecordMapper extends JdbcAvroIO.RowMapper {
      override def convert(resultSet: ResultSet, schema: Schema): GenericRecord = {
        JdbcAvroConversions.convertResultSetIntoAvroRecord(schema, resultSet)
      }
    }

    val jdbcAvroOptions = JdbcAvroIO.JdbcAvroOptions.create(
      JdbcAvroIO.DataSourceConfiguration.create(options.driverClass, options.connectionUrl)
        .withUsername(options.username)
        .withPassword(options.password), new ResultSetGenericRecordMapper())
    JdbcAvroIO.Write.createWrite(
      output,
      ".avro",
      generatedSchema,
      jdbcAvroOptions
    )
  }

  def publishMetrics(scioResult: ScioResult, output: String): Unit = {
    log.info(s"Metrics ${scioResult.getMetrics.toString}")
    val metrics: Map[MetricName, MetricValue[_]] = scioResult.allCounters ++ scioResult.allGauges
    log.info(s"all counters and gauges ${metrics.toString}")

    saveJsonObject(subPath(output, "/_METRICS.json"), metrics)
    scioResult.saveMetrics(subPath(output, "/_SERVICE_METRICS.json"))
  }

  private def writeToFile(filename: String, contents: ByteBuffer): Unit = {
    val resourceId = FileSystems.matchNewResource(filename, false)
    val out = FileSystems.create(resourceId, MimeTypes.TEXT)
    try {
      out.write(contents)
    } finally {
      if (out != null) {
        out.close()
      }
    }
  }

  private def saveJsonObject(filename: String, obj: Object): Unit = {
    val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
    writeToFile(filename, ByteBuffer.wrap(mapper.writeValueAsBytes(obj)))
  }

  private def saveString(filename: String, contents: String): Unit = {
    writeToFile(filename, ByteBuffer.wrap(contents.getBytes(Charset.defaultCharset())))
  }

  private def subPath(path: String, subPath: String): String =
    path.replaceAll("/+$", "") + subPath

  def prepareExport(sc: ScioContext, args: JdbcExportArgs, output: String): ScioContext = {
    require(output != null && output != "", "'output' must be defined")
    val generatedSchema: Schema = createSchema(sc, args)
    saveString(subPath(output, "/_AVRO_SCHEMA.avsc"), generatedSchema.toString(true))

    val queries: Iterable[String] = args.buildQueries()
    queries.zipWithIndex.foreach { case (q: String, n: Int) =>
      saveString(subPath(output, s"/_queries/query_${n}.sql"), q)
    }
    log.info(s"Running queries: $queries")

    sc
      .parallelize(queries)
      .internal
      .apply("JdbcAvroSave",
        jdbcAvroTransform(output, args, generatedSchema))
    sc
  }

  def runExport(sc: ScioContext, args: JdbcExportArgs, output: String): Unit = {
    prepareExport(sc, args, output)

    val scioResult: ScioResult = sc.close().waitUntilDone()
    publishMetrics(scioResult, output)
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc: ScioContext, jdbcExportArgs: JdbcExportArgs, output: String) =
      JdbcExportArgs.contextAndArgs(cmdlineArgs)
    runExport(sc, jdbcExportArgs, output)
  }
}
