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

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.spotify.dbeam.args.JdbcAvroArgs;
import com.spotify.dbeam.args.JdbcConnectionArgs;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

@AutoValue
public abstract class JdbcParquetArgs implements Serializable {

  private static final long serialVersionUID = 774966613L;

  public abstract JdbcConnectionArgs jdbcConnectionConfiguration();

  @Nullable
  public abstract JdbcAvroArgs.StatementPreparator statementPreparator();

  public abstract int fetchSize();

  public abstract String parquetCodec();

  public abstract int rowGroupSize();

  public abstract int pageSize();

  public abstract List<String> preCommand();

  public abstract String arrayMode();

  abstract Builder builder();

  public CompressionCodecName getCompressionCodecName() {
    switch (parquetCodec().toLowerCase()) {
      case "snappy":
        return CompressionCodecName.SNAPPY;
      case "gzip":
        return CompressionCodecName.GZIP;
      case "zstd":
        return CompressionCodecName.ZSTD;
      case "lz4":
        return CompressionCodecName.LZ4_RAW;
      case "none":
      case "uncompressed":
        return CompressionCodecName.UNCOMPRESSED;
      default:
        throw new IllegalArgumentException("Invalid parquetCodec: " + parquetCodec());
    }
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setJdbcConnectionConfiguration(JdbcConnectionArgs jdbcConnectionArgs);

    abstract Builder setStatementPreparator(JdbcAvroArgs.StatementPreparator statementPreparator);

    abstract Builder setFetchSize(int fetchSize);

    abstract Builder setParquetCodec(String parquetCodec);

    abstract Builder setRowGroupSize(int rowGroupSize);

    abstract Builder setPageSize(int pageSize);

    abstract Builder setPreCommand(List<String> preCommand);

    abstract Builder setArrayMode(String arrayMode);

    abstract JdbcParquetArgs build();
  }

  public static JdbcParquetArgs create(
      final JdbcConnectionArgs jdbcConnectionArgs,
      final int fetchSize,
      final String parquetCodec,
      final int rowGroupSize,
      final int pageSize,
      final List<String> preCommand,
      final String arrayMode) {
    Preconditions.checkArgument(
        parquetCodec.matches("snappy|gzip|zstd|lz4|none|uncompressed"),
        "Parquet codec should be one of: snappy, gzip, zstd, lz4, none, uncompressed");
    return new AutoValue_JdbcParquetArgs.Builder()
        .setJdbcConnectionConfiguration(jdbcConnectionArgs)
        .setFetchSize(fetchSize)
        .setParquetCodec(parquetCodec)
        .setRowGroupSize(rowGroupSize)
        .setPageSize(pageSize)
        .setPreCommand(preCommand)
        .setArrayMode(arrayMode)
        .build();
  }

  public static JdbcParquetArgs create(final JdbcConnectionArgs jdbcConnectionArgs) {
    return create(
        jdbcConnectionArgs,
        10000,
        "snappy",
        64 * 1024 * 1024,
        1024 * 1024,
        Collections.emptyList(),
        "typed_first_row");
  }
}
