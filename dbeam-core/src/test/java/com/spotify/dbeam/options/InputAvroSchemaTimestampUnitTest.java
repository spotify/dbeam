/*-
 * -\-\-
 * DBeam Core
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

package com.spotify.dbeam.options;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class InputAvroSchemaTimestampUnitTest {

  private static Schema record(final String fieldType) {
    return new Schema.Parser()
        .parse(
            "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"long\"},"
                + "{\"name\":\"ts\",\"type\":"
                + fieldType
                + "}]}");
  }

  private static String ts(final String logicalType) {
    return "{\"type\":\"long\",\"logicalType\":\"" + logicalType + "\"}";
  }

  private static String rejected(final Schema schema, final boolean micros) {
    return Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> JdbcExportArgsFactory.validateInputSchemaTimestampUnits(schema, micros))
        .getMessage();
  }

  private static void accepted(final Schema schema, final boolean micros) {
    JdbcExportArgsFactory.validateInputSchemaTimestampUnits(schema, micros);
  }

  @Test
  public void shouldAcceptMatchingUnits() {
    accepted(record(ts("timestamp-millis")), false);
    accepted(record(ts("timestamp-micros")), true);
  }

  @Test
  public void shouldAcceptPlainLong() {
    accepted(record("\"long\""), false);
    accepted(record("\"long\""), true);
    accepted(record("[\"null\",\"long\"]"), true);
  }

  @Test
  public void shouldRejectMillisWithFlag() {
    final String msg = rejected(record(ts("timestamp-millis")), true);
    Assertions.assertTrue(msg.contains("ts has logicalType timestamp-millis"), msg);
    Assertions.assertTrue(msg.contains("remove the flag"), msg);
  }

  @Test
  public void shouldRejectMicrosWithoutFlag() {
    final String msg = rejected(record(ts("timestamp-micros")), false);
    Assertions.assertTrue(msg.contains("ts has logicalType timestamp-micros"), msg);
    Assertions.assertTrue(msg.contains("set the flag"), msg);
  }

  @Test
  public void shouldRejectLocalAndNanosTimestamps() {
    for (String type :
        new String[] {
          "local-timestamp-millis", "local-timestamp-micros", "timestamp-nanos",
          "local-timestamp-nanos"
        }) {
      Assertions.assertTrue(rejected(record(ts(type)), false).contains(type), type);
      Assertions.assertTrue(rejected(record(ts(type)), true).contains(type), type);
    }
  }

  @Test
  public void shouldRejectNestedUnionArrayAndMapFields() {
    final String micros = ts("timestamp-micros");
    final Schema schema =
        record(
            "[\"null\",{\"type\":\"record\",\"name\":\"inner\",\"fields\":["
                + "{\"name\":\"a\",\"type\":[\"null\","
                + micros
                + "]},"
                + "{\"name\":\"b\",\"type\":{\"type\":\"array\",\"items\":"
                + micros
                + "}},"
                + "{\"name\":\"c\",\"type\":{\"type\":\"map\",\"values\":"
                + micros
                + "}}]}]");
    final String msg = rejected(schema, false);
    Assertions.assertTrue(msg.contains("ts.a has"), msg);
    Assertions.assertTrue(msg.contains("ts.b[] has"), msg);
    Assertions.assertTrue(msg.contains("ts.c{} has"), msg);
    accepted(schema, true);
  }
}
