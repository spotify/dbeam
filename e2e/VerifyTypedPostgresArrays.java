/*-
 * -\-\-
 * DBeam Core
 * --
 * Copyright (C) 2016 - 2026 Spotify AB
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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

/** Checks the Avro export of e2e/ddl.sql typed_postgres_array_test with --arrayMode=typed_postgres. */
public final class VerifyTypedPostgresArrays {

  private VerifyTypedPostgresArrays() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("Expected one Avro file path");
    }
    final TreeMap<Integer, GenericRecord> rows = new TreeMap<>();
    final Schema schema;
    try (DataFileReader<GenericRecord> reader =
        new DataFileReader<>(new File(args[0]), new GenericDatumReader<>())) {
      schema = reader.getSchema();
      while (reader.hasNext()) {
        final GenericRecord record = reader.next();
        rows.put((Integer) record.get("id"), record);
      }
    }
    final List<String> errors = new ArrayList<>();
    expectItemType(schema, "json_arr", Schema.Type.STRING, errors);
    expectItemType(schema, "jsonb_arr", Schema.Type.STRING, errors);
    expectItemType(schema, "bpchar_arr", Schema.Type.STRING, errors);
    expectItemType(schema, "bool_arr", Schema.Type.BOOLEAN, errors);
    expectItemType(schema, "float4_arr", Schema.Type.FLOAT, errors);
    expectItemType(schema, "float8_arr", Schema.Type.DOUBLE, errors);
    if (!rows.keySet().equals(new java.util.TreeSet<>(Arrays.asList(1, 2)))) {
      errors.add("expected rows with id 1 and 2, got " + rows.keySet());
    } else {
      final GenericRecord row = rows.get(1);
      // jsonb normalizes its text; json keeps it verbatim
      expect(row, "json_arr", Arrays.asList("{\"a\": 1}", "[1, 2]"), errors);
      expect(row, "jsonb_arr", Arrays.asList("{\"b\": true}", "\"x\""), errors);
      expect(row, "bpchar_arr", Arrays.asList("ab ", "xyz"), errors);
      expect(row, "bool_arr", Arrays.asList(true, false), errors);
      expect(row, "float4_arr", Arrays.asList(1.5f, -2.25f), errors);
      expect(row, "float8_arr", Arrays.asList(0.1d, 1e300d), errors);
      for (String field :
          Arrays.asList(
              "json_arr", "jsonb_arr", "bpchar_arr", "bool_arr", "float4_arr", "float8_arr")) {
        if (rows.get(2).get(field) != null) {
          errors.add("row 2 " + field + ": expected null, got " + rows.get(2).get(field));
        }
      }
    }
    if (!errors.isEmpty()) {
      throw new AssertionError("typed_postgres arrays:\n  " + String.join("\n  ", errors));
    }
  }

  private static void expectItemType(
      final Schema schema, final String field, final Schema.Type expected, final List<String> errors) {
    final Schema array = schema.getField(field).schema().getTypes().get(1);
    Schema items = array.getElementType();
    if (items.getType() == Schema.Type.UNION) {
      items = items.getTypes().get(0);
    }
    if (items.getType() != expected) {
      errors.add(field + ": expected items of type " + expected + ", schema has " + items);
    }
  }

  private static void expect(
      final GenericRecord row, final String field, final List<?> expected, final List<String> errors) {
    final Object value = row.get(field);
    final List<Object> actual = new ArrayList<>();
    if (value != null) {
      for (Object item : (List<?>) value) {
        // Avro strings are read as Utf8
        actual.add(item instanceof CharSequence ? item.toString() : item);
      }
    }
    if (!Objects.equals(actual, expected)) {
      errors.add(field + ": expected " + expected + ", got " + actual);
    }
  }
}
