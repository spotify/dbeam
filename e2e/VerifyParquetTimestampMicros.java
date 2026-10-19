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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

/** Reads Parquet output through its footer Avro schema, as Scio/Beam ParquetIO consumers do. */
public final class VerifyParquetTimestampMicros {

  private VerifyParquetTimestampMicros() {}

  public static void main(String[] args) throws Exception {
    if (args.length < 1 || args.length > 2) {
      throw new IllegalArgumentException(
          "Expected a Parquet file path and optionally the _AVRO_SCHEMA.avsc path");
    }

    // Expected values from e2e/ddl.sql timestamp_micros_test, as epoch microseconds
    final long base = 1787252761000000L; // 2026-08-20 19:06:01+00
    final List<Long> expected = List.of(base + 879000L, base + 879123L, base + 879456L);

    final List<Long> createdAtValues = new ArrayList<>();
    String logicalType = null;
    try (ParquetReader<GenericRecord> reader =
        AvroParquetReader.<GenericRecord>builder(
                HadoopInputFile.fromPath(new Path(args[0]), new Configuration()))
            .build()) {
      GenericRecord record;
      while ((record = reader.read()) != null) {
        Schema fieldSchema = record.getSchema().getField("created_at").schema();
        if (fieldSchema.getType() == Schema.Type.UNION) {
          for (Schema branch : fieldSchema.getTypes()) {
            if (branch.getType() != Schema.Type.NULL) {
              fieldSchema = branch;
            }
          }
        }
        logicalType = fieldSchema.getProp("logicalType");
        createdAtValues.add((Long) record.get("created_at"));
      }
    }

    if (args.length == 2) {
      verifyFooterMatchesAvsc(args[0], args[1]);
    }
    if (!"timestamp-micros".equals(logicalType)) {
      throw new AssertionError(
          "Expected logicalType=timestamp-micros in footer Avro schema, got: " + logicalType);
    }
    createdAtValues.sort(null);
    if (!createdAtValues.equals(expected)) {
      throw new AssertionError(
          "Expected created_at epoch micros " + expected + ", got " + createdAtValues);
    }
  }

  /** The footer Avro schema must be identical to the _AVRO_SCHEMA.avsc written next to it. */
  private static void verifyFooterMatchesAvsc(final String parquetPath, final String avscPath)
      throws Exception {
    final String footer;
    try (ParquetFileReader fileReader =
        ParquetFileReader.open(
            HadoopInputFile.fromPath(new Path(parquetPath), new Configuration()))) {
      footer =
          fileReader.getFooter().getFileMetaData().getKeyValueMetaData().get("parquet.avro.schema");
    }
    final Schema avsc =
        new Schema.Parser().parse(new String(Files.readAllBytes(Paths.get(avscPath))));
    if (footer == null || !avsc.equals(new Schema.Parser().parse(footer))) {
      throw new AssertionError(
          "Footer parquet.avro.schema " + footer + " differs from " + avscPath + ": " + avsc);
    }
  }
}
