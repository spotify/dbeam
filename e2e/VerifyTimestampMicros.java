/*-
 * -\-\-
 * DBeam Core
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

public final class VerifyTimestampMicros {

  private VerifyTimestampMicros() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("Expected one Avro file path");
    }

    final List<Long> createdAtValues = new ArrayList<>();
    String logicalType = null;
    try (DataFileReader<GenericRecord> reader =
        new DataFileReader<>(new File(args[0]), new GenericDatumReader<>())) {
      Schema.Field createdAtField = reader.getSchema().getField("created_at");
      for (Schema typeSchema : createdAtField.schema().getTypes()) {
        String lt = typeSchema.getProp("logicalType");
        if (lt != null) {
          logicalType = lt;
        }
      }
      while (reader.hasNext()) {
        createdAtValues.add((Long) reader.next().get("created_at"));
      }
    }

    if (!"timestamp-micros".equals(logicalType)) {
      throw new AssertionError(
          "Expected logicalType=timestamp-micros in auto-generated schema, got: " + logicalType);
    }
    if (createdAtValues.size() != 3) {
      throw new AssertionError("Expected 3 rows, found " + createdAtValues.size());
    }
    if (createdAtValues.get(0).equals(createdAtValues.get(1))) {
      throw new AssertionError(
          "row1 and row2 have identical created_at values: " + createdAtValues);
    }
    if (createdAtValues.get(0) % 1000 == 0 || createdAtValues.get(1) % 1000 == 0) {
      throw new AssertionError(
          "created_at values lost microsecond precision: " + createdAtValues);
    }

  }
}
