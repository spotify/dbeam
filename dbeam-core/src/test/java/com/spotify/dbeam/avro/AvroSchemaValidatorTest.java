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

package com.spotify.dbeam.avro;

import com.spotify.dbeam.jobs.FailedValidationException;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AvroSchemaValidatorTest {

  private static final String TS_MILLIS =
      "{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}";
  private static final String UUID = "{\"type\":\"string\",\"logicalType\":\"uuid\"}";

  private static Schema record(final String name, final String... fieldTypes) {
    final StringBuilder sb =
        new StringBuilder("{\"type\":\"record\",\"name\":\"" + name + "\",\"fields\":[");
    for (int i = 0; i < fieldTypes.length; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"name\":\"f").append(i).append("\",\"type\":").append(fieldTypes[i]).append('}');
    }
    return new Schema.Parser().parse(sb.append("]}").toString());
  }

  private static String nullable(final String type) {
    return "[\"null\"," + type + "]";
  }

  private static String array(final String items) {
    return "{\"type\":\"array\",\"items\":" + items + "}";
  }

  private static void accepted(final String provided, final String derived, final String logical)
      throws FailedValidationException {
    AvroSchemaValidator.validate(
        record("provided", provided), record("d", derived), record("dl", logical));
  }

  private static String rejected(
      final String provided, final String derived, final String logical) {
    return Assertions.assertThrows(
            FailedValidationException.class,
            () -> accepted(provided, derived, logical))
        .getMessage();
  }

  @Test
  public void shouldAcceptIdenticalSchema() throws FailedValidationException {
    accepted(nullable("\"long\""), nullable("\"long\""), nullable("\"long\""));
    accepted(nullable("\"string\""), nullable("\"string\""), nullable("\"string\""));
    accepted(nullable("\"bytes\""), nullable("\"bytes\""), nullable("\"bytes\""));
    accepted(nullable("\"int\""), nullable("\"int\""), nullable("\"int\""));
    accepted(nullable("\"boolean\""), nullable("\"boolean\""), nullable("\"boolean\""));
    accepted(nullable("\"float\""), nullable("\"float\""), nullable("\"float\""));
    accepted(nullable("\"double\""), nullable("\"double\""), nullable("\"double\""));
  }

  @Test
  public void shouldAcceptRenamesDocsAndNamespace() throws FailedValidationException {
    AvroSchemaValidator.validate(
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"other\",\"namespace\":\"x.y\",\"doc\":\"d\","
                    + "\"fields\":[{\"name\":\"renamed\",\"doc\":\"doc\","
                    + "\"type\":[\"null\",\"long\"]}]}"),
        record("d", nullable("\"long\"")),
        record("dl", nullable("\"long\"")));
  }

  @Test
  public void shouldAcceptTimestampLogicalTypeOrPlainLong() throws FailedValidationException {
    accepted(nullable("\"long\""), nullable("\"long\""), nullable(TS_MILLIS));
    accepted(nullable(TS_MILLIS), nullable("\"long\""), nullable(TS_MILLIS));
    accepted(nullable("\"long\""), nullable(TS_MILLIS), nullable(TS_MILLIS));
  }

  @Test
  public void shouldRejectTimestampLogicalTypeOnBigint() {
    final String msg = rejected(nullable(TS_MILLIS), nullable("\"long\""), nullable("\"long\""));
    Assertions.assertTrue(msg.contains("logicalType timestamp-millis is not compatible"), msg);
  }

  @Test
  public void shouldRejectOtherLogicalTypes() {
    rejected(
        nullable("{\"type\":\"int\",\"logicalType\":\"date\"}"),
        nullable("\"long\""),
        nullable(TS_MILLIS));
    rejected(
        nullable("{\"type\":\"long\",\"logicalType\":\"time-micros\"}"),
        nullable("\"long\""),
        nullable(TS_MILLIS));
    rejected(
        nullable("{\"type\":\"int\",\"logicalType\":\"time-millis\"}"),
        nullable("\"int\""),
        nullable("\"int\""));
    final String msg =
        rejected(
            nullable("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":4}"),
            nullable("\"string\""),
            nullable("\"string\""));
    Assertions.assertTrue(msg.contains("expected string but schema file has bytes"), msg);
    rejected(nullable(UUID), nullable("\"string\""), nullable("\"string\""));
  }

  @Test
  public void shouldAcceptUuidWithOrWithoutLogicalType() throws FailedValidationException {
    accepted(nullable(UUID), nullable("\"string\""), nullable(UUID));
    accepted(nullable("\"string\""), nullable(UUID), nullable(UUID));
  }

  @Test
  public void shouldRejectDifferentPhysicalTypes() {
    final String msg = rejected(nullable("\"int\""), nullable("\"long\""), nullable("\"long\""));
    Assertions.assertTrue(msg.contains("f0 (#1, JDBC column f0)"), msg);
    Assertions.assertTrue(msg.contains("expected long but schema file has int"), msg);
    rejected(nullable("\"float\""), nullable("\"double\""), nullable("\"double\""));
    rejected(nullable("\"string\""), nullable("\"bytes\""), nullable("\"bytes\""));
    rejected(
        nullable("{\"type\":\"fixed\",\"name\":\"u\",\"size\":16}"),
        nullable("\"string\""),
        nullable(UUID));
    rejected(
        nullable("{\"type\":\"enum\",\"name\":\"e\",\"symbols\":[\"A\"]}"),
        nullable("\"string\""),
        nullable("\"string\""));
  }

  @Test
  public void shouldRejectNonNullableOrReorderedUnion() {
    final String msg = rejected("\"long\"", nullable("\"long\""), nullable("\"long\""));
    Assertions.assertTrue(msg.contains("expected a union"), msg);
    rejected("[\"long\",\"null\"]", nullable("\"long\""), nullable("\"long\""));
    rejected("[\"null\",\"long\",\"string\"]", nullable("\"long\""), nullable("\"long\""));
  }

  @Test
  public void shouldRejectDifferentFieldCount() {
    final String msg =
        Assertions.assertThrows(
                FailedValidationException.class,
                () ->
                    AvroSchemaValidator.validate(
                        record("p", nullable("\"long\"")),
                        record("d", nullable("\"long\""), nullable("\"int\"")),
                        record("dl", nullable("\"long\""), nullable("\"int\""))))
            .getMessage();
    Assertions.assertTrue(msg.contains("expected 2 fields but schema file has 1"), msg);
  }

  @Test
  public void shouldCompareArrayItems() throws FailedValidationException {
    accepted(
        nullable(array("\"int\"")), nullable(array("\"int\"")), nullable(array("\"int\"")));
    accepted(
        nullable(array(UUID)), nullable(array("\"string\"")), nullable(array(UUID)));
    rejected(
        nullable(array("\"string\"")), nullable(array("\"int\"")), nullable(array("\"int\"")));
    final String msg =
        rejected(
            nullable(array("[\"int\",\"null\"]")),
            nullable(array("\"int\"")),
            nullable(array("\"int\"")));
    Assertions.assertTrue(msg.contains("--nullableArrayItems"), msg);
    accepted(
        nullable(array("[\"int\",\"null\"]")),
        nullable(array("[\"int\",\"null\"]")),
        nullable(array("[\"int\",\"null\"]")));
    rejected(
        nullable(array("[\"null\",\"int\"]")),
        nullable(array("[\"int\",\"null\"]")),
        nullable(array("[\"int\",\"null\"]")));
    rejected(
        nullable(array("\"int\"")),
        nullable(array("[\"int\",\"null\"]")),
        nullable(array("[\"int\",\"null\"]")));
    rejected(nullable("\"bytes\""), nullable(array("\"int\"")), nullable(array("\"int\"")));
  }

  @Test
  public void shouldAcceptAnyItemsWhenTypedFirstRowItemTypeIsUnknown()
      throws FailedValidationException {
    accepted(
        nullable(array("\"long\"")), nullable(array("\"null\"")), nullable(array("\"null\"")));
  }
}
