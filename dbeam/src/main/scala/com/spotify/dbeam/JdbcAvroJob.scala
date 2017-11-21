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
import java.sql.ResultSet

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.spotify.scio._
import com.spotify.scio.metrics.MetricValue
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.coders.AvroCoder
import org.apache.beam.sdk.io.{FileBasedSink, FileSystems, WriteFiles}
import org.apache.beam.sdk.metrics.MetricName
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.util.MimeTypes
import org.apache.beam.sdk.values.{PCollection, PDone}
import org.slf4j.{Logger, LoggerFactory}

object JdbcAvroJob {
  val log: Logger = LoggerFactory.getLogger(JdbcAvroJob.getClass)

  /**
    *  Generate Avro schema by reading one row
    *  Also save schema to output target and expose time to generate schema as a Beam counter
    */
  def createSchema(sc: ScioContext, options: SqlAvroOptions): Schema = {
    val startTimeMillis: Long = System.currentTimeMillis()
    val generatedSchema: Schema = JdbcAvroConversions.createSchemaByReadingOneRow(
      options.createConnection(),
      options.tableName, options.avroSchemaNamespace)
    val elapsedTimeSchema: Long = (System.currentTimeMillis() - startTimeMillis)
    log.info(s"Elapsed time to schema ${elapsedTimeSchema/ 1000.0} seconds")
    sc
      .parallelize(Seq(0))
      .withName("ExposeSchemaCounters")
      .map(inp => {
        ScioMetrics.counter("schemaElapsedTimeMs").inc(elapsedTimeSchema)
        inp
      })
    saveString(args.pathInOutput("/_AVRO_SCHEMA.avsc"), generatedSchema.toString(true))
    generatedSchema
  }

  /**
    * Creates Beam transform to read data from JDBC and save to Avro, in a single step
    */
  def jdbcAvroTransform(output: String,
                        options: JdbcConnectionOptions,
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

  def publishMetrics(scioResult: ScioResult, options: SqlAvroOptions): Unit = {
    log.info(s"Metrics ${scioResult.getMetrics.toString}")
    val metrics: Map[MetricName, MetricValue[_]] = scioResult.allCounters ++ scioResult.allGauges
    log.info(s"all counters and gauges ${metrics.toString}")

    saveJsonObject(options.pathInOutput("/_METRICS.json"), metrics)
    scioResult.saveMetrics(options.pathInOutput("/_SERVICE_METRICS.json"))
  }

  def writeToFile(filename: String, contents: ByteBuffer): Unit = {
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

  def saveJsonObject(filename: String, obj: Object): Unit = {
    val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
    writeToFile(filename, ByteBuffer.wrap(mapper.writeValueAsBytes(obj)))
  }

  def saveString(filename: String, contents: String): Unit = {
    writeToFile(filename, ByteBuffer.wrap(contents.getBytes(Charset.defaultCharset())))
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc: ScioContext, options: SqlAvroOptions) = SqlAvroOptions.contextAndOptions(cmdlineArgs)
    val generatedSchema: Schema = createSchema(sc, options)

    val queries = options.buildQueries
    log.info(s"Running queries: ${queries}")

    val querySCollection = sc.parallelize(queries)
    querySCollection
      .withName("SaveQueries")
      .saveAsTextFile(options.pathInOutput("/_queries"), ".sql")
    querySCollection
      .internal.apply("JdbcAvroSave", jdbcAvroTransform(options.output, options, generatedSchema))

    val scioResult: ScioResult = sc.close().waitUntilDone()
    publishMetrics(scioResult, options)
  }

}
