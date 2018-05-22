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

import org.apache.beam.sdk.options.Validation.Required
import org.apache.beam.sdk.options.{Default, Description, PipelineOptions}

import scala.io.Source

trait DBeamPipelineOptions extends PipelineOptions {
  @Description("The JDBC connection url to perform the extraction on.")
  @Required
  def getConnectionUrl: String

  def setConnectionUrl(value: String): Unit

  @Description("The database table to query and perform the extraction on.")
  @Required
  def getTable: String

  def setTable(value: String): Unit

  @Description("The database user name used by JDBC to authenticate.")
  @Default.String("dbeam-extractor")
  def getUsername: String

  def setUsername(value: String): Unit

  @Description("A path to a local file containing the database password.")
  def getPasswordFile: String

  def setPasswordFile(value: String): Unit

  @Description("Database password")
  def getPassword: String

  def setPassword(value: String): Unit
}

@Description("Configure dbeam SQL export")
trait JdbcExportPipelineOptions extends DBeamPipelineOptions {
  @Description("The date of the current partition.")
  def getPartition: String

  def setPartition(value: String): Unit

  @Description("The name of a date/timestamp column to filter data based on current partition.")
  def getPartitionColumn: String

  def setPartitionColumn(value: String): Unit

  @Description("By default, when partition column is not specified, " +
    "fails if partition is too old. Set this flag to ignore this check.")
  @Default.Boolean(false)
  def isSkipPartitionCheck: Boolean

  def setSkipPartitionCheck(value: Boolean): Unit

  @Description("The minimum partition required for the job not to fail " +
    "(when partition column is not specified), by default `now() - 2*partitionPeriod`.")
  def getPartitionPeriod: String

  def setPartitionPeriod(value: String): Unit

  def getMinPartitionPeriod: String

  def setMinPartitionPeriod(value: String): Unit

  @Description("Limit the output number of rows, indefinite by default.")
  def getLimit: Integer

  def setLimit(value: Integer): Unit

  @Description("The namespace of the generated avro schema.")
  @Default.String("dbeam_generated")
  def getAvroSchemaNamespace: String

  def setAvroSchemaNamespace(value: String): Unit

  @Description("The top-level doc string of the generated avro schema.")
  def getAvroDoc: String

  def setAvroDoc(value: String): Unit

  @Default.Boolean(false)
  @Description(
    "Controls whether generated Avro schema will contain logicalTypes or not.")
  def isUseAvroLogicalTypes: Boolean

  def setUseAvroLogicalTypes(value: Boolean): Unit
}

trait OutputOptions extends PipelineOptions {
  @Description("The path for storing the output.")
  @Required
  def getOutput: String

  def setOutput(value: String): Unit
}

object PipelineOptionsUtil {
  def readPassword(options: DBeamPipelineOptions): Option[String] = {
    Option(options.getPasswordFile).map(Source.fromFile(_).mkString.stripLineEnd)
      .orElse(Option(options.getPassword))
  }
}
