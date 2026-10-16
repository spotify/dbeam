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

import com.spotify.dbeam.args.JdbcConnectionArgs;
import java.util.Collections;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JdbcParquetArgsTest {

  private static JdbcConnectionArgs connArgs() {
    try {
      return JdbcConnectionArgs.create("jdbc:h2:mem:test");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private static final JdbcConnectionArgs CONN_ARGS = connArgs();

  @Test
  public void shouldCreateWithDefaults() {
    final JdbcParquetArgs args = JdbcParquetArgs.create(CONN_ARGS);

    Assertions.assertEquals(10000, args.fetchSize());
    Assertions.assertEquals("snappy", args.parquetCodec());
    Assertions.assertEquals(128L * 1024 * 1024, args.rowGroupSize());
    Assertions.assertEquals(1024L * 1024, args.pageSize());
    Assertions.assertEquals(Collections.emptyList(), args.preCommand());
  }

  @Test
  public void shouldMapSnappyCodec() {
    final JdbcParquetArgs args = JdbcParquetArgs.create(CONN_ARGS);

    Assertions.assertEquals(CompressionCodecName.SNAPPY, args.getCompressionCodecName());
  }

  @Test
  public void shouldMapGzipCodec() {
    final JdbcParquetArgs args =
        JdbcParquetArgs.create(
            CONN_ARGS,
            10000,
            "gzip",
            64 * 1024 * 1024,
            1024 * 1024,
            Collections.emptyList(),
            "typed_first_row");

    Assertions.assertEquals(CompressionCodecName.GZIP, args.getCompressionCodecName());
  }

  @Test
  public void shouldMapZstdCodec() {
    final JdbcParquetArgs args =
        JdbcParquetArgs.create(
            CONN_ARGS,
            10000,
            "zstd",
            64 * 1024 * 1024,
            1024 * 1024,
            Collections.emptyList(),
            "typed_first_row");

    Assertions.assertEquals(CompressionCodecName.ZSTD, args.getCompressionCodecName());
  }

  @Test
  public void shouldMapLz4Codec() {
    final JdbcParquetArgs args =
        JdbcParquetArgs.create(
            CONN_ARGS,
            10000,
            "lz4",
            64 * 1024 * 1024,
            1024 * 1024,
            Collections.emptyList(),
            "typed_first_row");

    Assertions.assertEquals(CompressionCodecName.LZ4_RAW, args.getCompressionCodecName());
  }

  @Test
  public void shouldMapUncompressedCodec() {
    final JdbcParquetArgs args =
        JdbcParquetArgs.create(
            CONN_ARGS,
            10000,
            "uncompressed",
            64 * 1024 * 1024,
            1024 * 1024,
            Collections.emptyList(),
            "typed_first_row");

    Assertions.assertEquals(CompressionCodecName.UNCOMPRESSED, args.getCompressionCodecName());
  }

  @Test
  public void shouldMapNoneCodec() {
    final JdbcParquetArgs args =
        JdbcParquetArgs.create(
            CONN_ARGS,
            10000,
            "none",
            64 * 1024 * 1024,
            1024 * 1024,
            Collections.emptyList(),
            "typed_first_row");

    Assertions.assertEquals(CompressionCodecName.UNCOMPRESSED, args.getCompressionCodecName());
  }

  @Test
  public void shouldRejectInvalidCodec() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            JdbcParquetArgs.create(
                CONN_ARGS,
                10000,
                "invalid",
                64 * 1024 * 1024,
                1024 * 1024,
                Collections.emptyList(),
                "typed_first_row"));
  }

  @Test
  public void shouldAcceptCustomFetchSize() {
    final JdbcParquetArgs args =
        JdbcParquetArgs.create(
            CONN_ARGS,
            50000,
            "snappy",
            64 * 1024 * 1024,
            1024 * 1024,
            Collections.emptyList(),
            "typed_first_row");

    Assertions.assertEquals(50000, args.fetchSize());
  }

  @Test
  public void shouldAcceptCustomRowGroupSize() {
    final JdbcParquetArgs args =
        JdbcParquetArgs.create(
            CONN_ARGS,
            10000,
            "snappy",
            128 * 1024 * 1024,
            1024 * 1024,
            Collections.emptyList(),
            "typed_first_row");

    Assertions.assertEquals(128L * 1024 * 1024, args.rowGroupSize());
  }
}
