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

import java.util.List;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

@Description("Configures the DBeam SQL export")
public interface JdbcExportPipelineOptions extends DBeamPipelineOptions {

  @Description("The date/timestamp of the current partition.")
  String getPartition();

  void setPartition(String value);

  @Description("The name of a date/timestamp column to filter data based on current partition.")
  String getPartitionColumn();

  void setPartitionColumn(String value);

  @Default.Boolean(false)
  @Description(
      "When partition column is not specified, "
          + "fails if partition is too old; set this flag to ignore this check.")
  Boolean isSkipPartitionCheck();

  void setSkipPartitionCheck(Boolean value);

  @Description(
      "The period frequency which the export runs, used to filter based "
          + "on current partition and also to check if exports are running for too old partitions.")
  String getPartitionPeriod();

  void setPartitionPeriod(String value);

  @Description(
      "The minimum partition required for the job not to fail "
          + "(when partition column is not specified),"
          + "by default `now() - 2*partitionPeriod`.")
  String getMinPartitionPeriod();

  void setMinPartitionPeriod(String value);

  @Description("Limit the output number of rows, indefinite by default.")
  Long getLimit();

  void setLimit(Long value);

  @Default.String("dbeam_generated")
  @Description("The namespace of the generated avro schema.")
  String getAvroSchemaNamespace();

  void setAvroSchemaNamespace(String value);

  @Description("The name of the generated avro schema. The table name by default.")
  String getAvroSchemaName();

  void setAvroSchemaName(String value);

  @Description("The top-level record doc string of the generated avro schema.")
  String getAvroDoc();

  void setAvroDoc(String value);

  @Default.Boolean(false)
  @Description("Controls whether generated Avro schema will contain logicalTypes or not.")
  Boolean isUseAvroLogicalTypes();

  void setUseAvroLogicalTypes(Boolean value);

  @Default.String("typed_first_row")
  @Description("Configures how arrays are treated: bytes, typed_first_row, typed_postgres.")
  String getArrayMode();

  void setArrayMode(String value);

  @Default.Boolean(false)
  @Description("Controls whether array items should be nullable, ignored if arrayMode is 'bytes'.")
  Boolean isNullableArrayItems();

  void setNullableArrayItems(Boolean value);

  @Default.Integer(10000)
  @Description("Configures JDBC Statement fetch size.")
  Integer getFetchSize();

  void setFetchSize(Integer value);

  @Default.String("deflate6")
  @Description("Avro codec (e.g. deflate6, deflate9, snappy).")
  String getAvroCodec();

  void setAvroCodec(String value);

  @Description(
      "A long/integer column used to create splits for parallel queries. "
          + "Should be used with queryParallelism.")
  String getSplitColumn();

  void setSplitColumn(String value);

  @Description(
      "Max number of queries to run in parallel for exports. "
          + "Single query used if nothing specified. Should be used with splitColumn.")
  Integer getQueryParallelism();

  void setQueryParallelism(Integer value);

  @Default.String("P7D")
  @Description(
      "Export timeout, after this duration the job is cancelled and the export terminated.")
  String getExportTimeout();

  void setExportTimeout(String value);

  @Description("Path to file with a target AVRO schema.")
  String getAvroSchemaFilePath();

  void setAvroSchemaFilePath(String value);

  @Description("SQL commands to be executed before query.")
  List<String> getPreCommand();

  void setPreCommand(List<String> value);

  @Description(
      "Check that the output has at least this minimum number of rows. "
          + "Otherwise fail the job.")
  @Default.Long(-1)
  Long getMinRows();

  void setMinRows(Long value);
}
