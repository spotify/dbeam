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

import com.spotify.dbeam.options.{JdbcAvroOptions, JdbcExportArgs}
import org.apache.avro.Schema
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.transforms.{Create, PTransform}
import org.apache.beam.sdk.values.{PCollection, POutput}
import org.apache.beam.sdk.{Pipeline, PipelineResult}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object JdbcAvroJob {
  val log: Logger = LoggerFactory.getLogger(JdbcAvroJob.getClass)

  /**
    * Creates Beam transform to read data from JDBC and save to Avro, in a single step
    */
  def jdbcAvroTransform(output: String,
                        jdbcAvroOptions: JdbcAvroOptions,
                        generatedSchema: Schema): PTransform[PCollection[String], _ <: POutput] = {
    JdbcAvroIO.Write.createWrite(
      output,
      ".avro",
      generatedSchema,
      jdbcAvroOptions
    )
  }

  def prepareExport(p: Pipeline, args: JdbcExportArgs, output: String): Unit = {
    require(output != null && output != "", "'output' must be defined")
    val generatedSchema: Schema = BeamJdbcAvroSchema.createSchema(p, args)
    BeamHelper.saveStringOnSubPath(output, "/_AVRO_SCHEMA.avsc", generatedSchema.toString(true))

    val queries: Iterable[String] = args.buildQueries().asScala
    queries.zipWithIndex.foreach { case (q: String, n: Int) =>
      BeamHelper.saveStringOnSubPath(output, s"/_queries/query_$n.sql", q)
    }
    log.info("Running queries: {}", queries.toString())

    p.apply("JdbcQueries", Create.of(queries.asJava))
      .apply("JdbcAvroSave", jdbcAvroTransform(output, args.jdbcAvroOptions, generatedSchema))
  }

  def runExport(opts: PipelineOptions, args: JdbcExportArgs, output: String): Unit = {
    val pipeline: Pipeline = Pipeline.create(opts)
    prepareExport(pipeline, args, output)
    val result: PipelineResult = BeamHelper.waitUntilDone(pipeline.run())
    BeamHelper.saveMetrics(MetricsHelper.getMetrics(result), output)
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    val (opts: PipelineOptions, jdbcExportArgs: JdbcExportArgs, output: String) =
      JdbcExportArgs.parseOptions(cmdlineArgs)
    runExport(opts, jdbcExportArgs, output)
  }
}
