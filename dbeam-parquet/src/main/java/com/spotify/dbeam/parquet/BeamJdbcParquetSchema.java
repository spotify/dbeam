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

import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Optional;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BeamJdbcParquetSchema {

  private static final Logger LOGGER = LoggerFactory.getLogger(BeamJdbcParquetSchema.class);

  public static void exposeSchemaMetrics(final Pipeline pipeline, final long elapsedNanos) {
    final long elapsedMs = elapsedNanos / 1000000;
    LOGGER.info("Elapsed time to schema {} seconds", elapsedMs / 1000.0);
    final Counter cnt =
        Metrics.counter(BeamJdbcParquetSchema.class.getCanonicalName(), "schemaElapsedTimeMs");
    pipeline
        .apply(
            "ExposeSchemaCountersSeed",
            Create.of(Collections.singletonList(0)).withType(TypeDescriptors.integers()))
        .apply(
            "ExposeSchemaCounters",
            MapElements.into(TypeDescriptors.integers())
                .via(
                    v -> {
                      cnt.inc(elapsedMs);
                      return v;
                    }));
  }

  public static Optional<MessageType> parseOptionalInputParquetSchemaFile(final String filename)
      throws IOException {
    if (filename == null || filename.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(parseInputParquetSchemaFile(filename));
  }

  public static MessageType parseInputParquetSchemaFile(final String filename) throws IOException {
    final MatchResult.Metadata m = FileSystems.matchSingleFileSpec(filename);
    try (InputStream inputStream = Channels.newInputStream(FileSystems.open(m.resourceId()));
        InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
      return MessageTypeParser.parseMessageType(CharStreams.toString(reader));
    }
  }
}
