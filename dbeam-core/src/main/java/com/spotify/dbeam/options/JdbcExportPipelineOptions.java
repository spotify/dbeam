/*-
 * -\-\-
 * DBeam Core
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.dbeam.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

@Description("Configure dbeam SQL export")
public interface JdbcExportPipelineOptions extends DBeamPipelineOptions {

  @Description("The date of the current partition.")
  String getPartition();

  void setPartition(String value);

  @Description("The name of a date/timestamp column to filter data based on current partition.")
  String getPartitionColumn();

  void setPartitionColumn(String value);

  @Description("By default, when partition column is not specified, "
      + "fails if partition is too old. Set this flag to ignore this check.")
  @Default.Boolean(false)
  Boolean isSkipPartitionCheck();

  void setSkipPartitionCheck(Boolean value);

  @Description("The minimum partition required for the job not to fail "
      + "(when partition column is not specified),"
      + "by default `now() - 2*partitionPeriod`.")
  String getPartitionPeriod();

  void setPartitionPeriod(String value);

  String getMinPartitionPeriod();

  void setMinPartitionPeriod(String value);

  @Description("Limit the output number of rows, indefinite by default.")
  Integer getLimit();

  void setLimit(Integer value);

  @Description("The namespace of the generated avro schema.")
  @Default.String("dbeam_generated")
  String getAvroSchemaNamespace();

  void setAvroSchemaNamespace(String value);

  @Description("The top-level doc string of the generated avro schema.")
  String getAvroDoc();

  void setAvroDoc(String value);

  @Default.Boolean(false)
  @Description(
      "Controls whether generated Avro schema will contain logicalTypes or not.")
  Boolean isUseAvroLogicalTypes();

  void setUseAvroLogicalTypes(Boolean value);

  @Default.Integer(10000)
  @Description(
      "Jdbc result set fetch size.")
  Integer getFetchSize();

  void setFetchSize(Integer value);

  @Default.String("deflate6")
  @Description(
      "Avro codec (e.g. deflate6, deflate9, snappy).")
  String getAvroCodec();

  void setAvroCodec(String value);

  @Description(
      "Column used to create splits in case of parallel exports. "
          + "Should be used with queryParallelism")
  String getSplitColumn();

  void setSplitColumn(String value);

  @Description(
      "Number of queries to run in parallel for exports. Should be used with splitColumn")
  Integer getQueryParallelism();

  void setQueryParallelism(Integer value);

  @Default.String("PT0S")
  @Description(
      "Export timeout, after this duration the export will be terminated.")
  String getExportTimeout();

  void setExportTimeout(String value);
}
