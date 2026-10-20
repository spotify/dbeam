/*-
 * -\-\-
 * DBeam Parquet
 * --
 * Copyright (C) 2016 - 2025 Spotify AB
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

package com.spotify.dbeam.parquet;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

@Description("Parquet-specific export options")
public interface ParquetPipelineOptions extends PipelineOptions {

  @Description(
      "Path to file with a target Parquet schema (MessageType text format). Must match the"
          + " JDBC-derived schema except for field names and compatible annotations.")
  String getParquetSchemaFilePath();

  void setParquetSchemaFilePath(String value);

  @Default.String("snappy")
  @Description(
      "Parquet compression codec: snappy, gzip, zstd, zstd1 .. zstd22 (zstd with compression"
          + " level), lz4, none, uncompressed. --avroCodec is ignored by Parquet exports.")
  String getParquetCodec();

  void setParquetCodec(String value);

  @Description(
      "Parquet row group size in bytes. Larger values improve read performance "
          + "but use more memory during writes.")
  @Default.Long(134217728)
  Long getRowGroupSize();

  void setRowGroupSize(Long value);

  @Description(
      "Parquet page size in bytes. Controls the granularity of encoding "
          + "and compression within a column chunk.")
  @Default.Long(1048576)
  Long getPageSize();

  void setPageSize(Long value);
}
