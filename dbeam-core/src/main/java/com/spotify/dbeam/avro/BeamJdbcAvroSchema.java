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

package com.spotify.dbeam.avro;

import com.spotify.dbeam.args.JdbcExportArgs;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import org.apache.avro.Schema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BeamJdbcAvroSchema {

  private static final Logger LOGGER = LoggerFactory.getLogger(BeamJdbcAvroSchema.class);

  /**
   * Generate Avro schema by reading one row. Expose Beam metrics via a Beam PTransform.
   *
   * @param pipeline Beam SDK pipeline, to expose metrics
   * @param args JdbcExportArgs with DBeam configuration
   * @param connection JDBC connection to query input schema
   * @return Avro schema based on configuration
   * @throws Exception in case of failure to query database
   */
  public static Schema createSchema(
      final Pipeline pipeline,
      final JdbcExportArgs args,
      final Connection connection,
      final AvroSchemaMetadataProvider provider)
      throws Exception {
    final long startTime = System.nanoTime();
    final Schema generatedSchema = generateAvroSchema(args, connection, provider);
    final long elapsedTimeSchema = (System.nanoTime() - startTime) / 1000000;
    LOGGER.info("Elapsed time to schema {} seconds", elapsedTimeSchema / 1000.0);

    final Counter cnt =
        Metrics.counter(BeamJdbcAvroSchema.class.getCanonicalName(), "schemaElapsedTimeMs");
    pipeline
        .apply(
            "ExposeSchemaCountersSeed",
            Create.of(Collections.singletonList(0)).withType(TypeDescriptors.integers()))
        .apply(
            "ExposeSchemaCounters",
            MapElements.into(TypeDescriptors.integers())
                .via(
                    v -> {
                      cnt.inc(elapsedTimeSchema);
                      return v;
                    }));
    return generatedSchema;
  }

  private static Schema generateAvroSchema(
      final JdbcExportArgs args,
      final Connection connection,
      final AvroSchemaMetadataProvider provider)
      throws SQLException {
    return JdbcAvroSchema.createSchemaByReadingOneRow(
        connection, args.queryBuilderArgs(), args.useAvroLogicalTypes(), provider);
  }

  public static Schema parseInputAvroSchemaFile(final String filename) throws IOException {
    final MatchResult.Metadata m = FileSystems.matchSingleFileSpec(filename);
    final InputStream inputStream = Channels.newInputStream(FileSystems.open(m.resourceId()));

    final Schema schema = new Schema.Parser().parse(inputStream);

    LOGGER.info("Parsed the provided schema from: [{}]", filename);

    return schema;
  }

}
