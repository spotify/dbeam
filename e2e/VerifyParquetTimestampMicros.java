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

import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

/** Reads Parquet output through its footer Avro schema, as Scio/Beam ParquetIO consumers do. */
public final class VerifyParquetTimestampMicros {

  private VerifyParquetTimestampMicros() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("Expected one Parquet file path");
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
}
