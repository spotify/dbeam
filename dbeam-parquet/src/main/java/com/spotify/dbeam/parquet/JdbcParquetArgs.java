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
import java.util.Optional;
import java.util.regex.Pattern;
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

  public abstract long rowGroupSize();

  public abstract long pageSize();

  public abstract List<String> preCommand();

  public abstract String arrayMode();

  /** Write timestamp/date/time columns as epoch micros instead of millis. */
  public abstract boolean useTimestampMicros();

  /** {@code --parquetNativeTypes}: native Parquet DATE, TIME and DECIMAL types. */
  public abstract boolean useNativeTypes();

  abstract Builder builder();

  static final String SUPPORTED_CODECS =
      "snappy, gzip, zstd, zstd1 .. zstd22 (zstd with compression level), lz4, none, uncompressed";

  private static final Pattern CODEC_PATTERN =
      Pattern.compile("snappy|gzip|zstd([1-9]|1[0-9]|2[0-2])?|lz4|none|uncompressed");

  /**
   * Fail with the list of supported values if {@code parquetCodec} is not one of them.
   *
   * @throws IllegalArgumentException for an unsupported codec
   */
  public static void validateParquetCodec(final String parquetCodec) {
    Preconditions.checkArgument(
        parquetCodec != null && CODEC_PATTERN.matcher(parquetCodec.toLowerCase()).matches(),
        "Invalid --parquetCodec '%s'. Supported values: %s",
        parquetCodec,
        SUPPORTED_CODECS);
  }

  /** Zstandard compression level from {@code zstd<level>}, empty for the codec's default. */
  public Optional<Integer> zstdLevel() {
    final String codec = parquetCodec().toLowerCase();
    if (codec.startsWith("zstd") && codec.length() > 4) {
      return Optional.of(Integer.parseInt(codec.substring(4)));
    }
    return Optional.empty();
  }

  public CompressionCodecName getCompressionCodecName() {
    final String codec = parquetCodec().toLowerCase();
    switch (codec.startsWith("zstd") ? "zstd" : codec) {
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

    abstract Builder setRowGroupSize(long rowGroupSize);

    abstract Builder setPageSize(long pageSize);

    abstract Builder setPreCommand(List<String> preCommand);

    abstract Builder setArrayMode(String arrayMode);

    abstract Builder setUseTimestampMicros(boolean useTimestampMicros);

    abstract Builder setUseNativeTypes(boolean useNativeTypes);

    abstract JdbcParquetArgs build();
  }

  public static JdbcParquetArgs create(
      final JdbcConnectionArgs jdbcConnectionArgs,
      final int fetchSize,
      final String parquetCodec,
      final long rowGroupSize,
      final long pageSize,
      final List<String> preCommand,
      final String arrayMode) {
    return create(
        jdbcConnectionArgs,
        fetchSize,
        parquetCodec,
        rowGroupSize,
        pageSize,
        preCommand,
        arrayMode,
        false);
  }

  public static JdbcParquetArgs create(
      final JdbcConnectionArgs jdbcConnectionArgs,
      final int fetchSize,
      final String parquetCodec,
      final long rowGroupSize,
      final long pageSize,
      final List<String> preCommand,
      final String arrayMode,
      final boolean useTimestampMicros) {
    return create(
        jdbcConnectionArgs,
        fetchSize,
        parquetCodec,
        rowGroupSize,
        pageSize,
        preCommand,
        arrayMode,
        useTimestampMicros,
        false);
  }

  public static JdbcParquetArgs create(
      final JdbcConnectionArgs jdbcConnectionArgs,
      final int fetchSize,
      final String parquetCodec,
      final long rowGroupSize,
      final long pageSize,
      final List<String> preCommand,
      final String arrayMode,
      final boolean useTimestampMicros,
      final boolean useNativeTypes) {
    validateParquetCodec(parquetCodec);
    return new AutoValue_JdbcParquetArgs.Builder()
        .setJdbcConnectionConfiguration(jdbcConnectionArgs)
        .setFetchSize(fetchSize)
        .setParquetCodec(parquetCodec)
        .setRowGroupSize(rowGroupSize)
        .setPageSize(pageSize)
        .setPreCommand(preCommand)
        .setArrayMode(arrayMode)
        .setUseTimestampMicros(useTimestampMicros)
        .setUseNativeTypes(useNativeTypes)
        .build();
  }

  public static JdbcParquetArgs create(final JdbcConnectionArgs jdbcConnectionArgs) {
    return create(
        jdbcConnectionArgs,
        10000,
        "snappy",
        128 * 1024 * 1024,
        1024 * 1024,
        Collections.emptyList(),
        "typed_first_row");
  }
}
