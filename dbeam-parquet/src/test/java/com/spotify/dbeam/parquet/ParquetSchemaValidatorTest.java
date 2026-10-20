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

import com.spotify.dbeam.jobs.FailedValidationException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ParquetSchemaValidatorTest {

  private static final MessageType DERIVED =
      MessageTypeParser.parseMessageType(
          "message t {\n"
              + "  optional binary name (STRING);\n"
              + "  optional int32 qty;\n"
              + "  optional int64 total;\n"
              + "  optional int64 created;\n"
              + "  optional binary raw;\n"
              + "  optional binary id (STRING);\n"
              + "  optional group tags (LIST) {\n"
              + "    repeated group list {\n"
              + "      optional int32 element;\n"
              + "    }\n"
              + "  }\n"
              + "}");

  private static final MessageType DERIVED_LOGICAL =
      MessageTypeParser.parseMessageType(
          "message t {\n"
              + "  optional binary name (STRING);\n"
              + "  optional int32 qty;\n"
              + "  optional int64 total;\n"
              + "  optional int64 created (TIMESTAMP(MILLIS,true));\n"
              + "  optional binary raw;\n"
              + "  optional fixed_len_byte_array(16) id (UUID);\n"
              + "  optional group tags (LIST) {\n"
              + "    repeated group list {\n"
              + "      optional int32 element;\n"
              + "    }\n"
              + "  }\n"
              + "}");

  private static String schema(final String... fields) {
    return "message renamed {\n" + String.join("\n", fields) + "\n}";
  }

  private static final String NAME = "optional binary name (STRING);";
  private static final String QTY = "optional int32 qty;";
  private static final String TOTAL = "optional int64 total;";
  private static final String CREATED = "optional int64 created;";
  private static final String RAW = "optional binary raw;";
  private static final String ID = "optional binary id (STRING);";
  private static final String TAGS =
      "optional group tags (LIST) { repeated group list { optional int32 element; } }";

  private static void validate(final String text) throws FailedValidationException {
    ParquetSchemaValidator.validate(
        MessageTypeParser.parseMessageType(text), DERIVED, DERIVED_LOGICAL);
  }

  private static String rejected(final String text) {
    return Assertions.assertThrows(FailedValidationException.class, () -> validate(text))
        .getMessage();
  }

  @Test
  public void shouldAcceptIdenticalSchemas() throws Exception {
    ParquetSchemaValidator.validate(DERIVED, DERIVED, DERIVED_LOGICAL);
    ParquetSchemaValidator.validate(DERIVED_LOGICAL, DERIVED, DERIVED_LOGICAL);
  }

  @Test
  public void shouldAcceptRenamesAndCompatibleAnnotations() throws Exception {
    validate(
        schema(
            "optional binary new_name (JSON);",
            "optional int32 quantity (INTEGER(32,true));",
            "optional int64 sum (INTEGER(64,true));",
            "optional int64 created_at (TIMESTAMP(MILLIS,true));",
            "optional binary bytes;",
            "optional fixed_len_byte_array(16) uuid (UUID);",
            "optional group labels (LIST) { repeated group items { optional int32 item; } }"));
    validate(
        schema(
            "optional binary name (ENUM);",
            QTY,
            TOTAL,
            CREATED,
            RAW,
            "optional binary id;",
            TAGS));
  }

  @Test
  public void shouldRejectFieldCountMismatch() {
    final String msg = rejected(schema(NAME, QTY, TOTAL, CREATED, RAW, ID));
    Assertions.assertTrue(msg.contains("expected 7 fields but schema file has 6"), msg);
  }

  @Test
  public void shouldRejectPhysicalTypeChange() {
    final String msg = rejected(schema(NAME, "optional int64 qty;", TOTAL, CREATED, RAW, ID, TAGS));
    Assertions.assertTrue(msg.contains("qty (#2, JDBC column qty)"), msg);
    Assertions.assertTrue(msg.contains("expected int32 but schema file has int64"), msg);
  }

  @Test
  public void shouldRejectRepetitionChange() {
    final String msg =
        rejected(schema("required binary name (STRING);", QTY, TOTAL, CREATED, RAW, ID, TAGS));
    Assertions.assertTrue(
        msg.contains("expected repetition OPTIONAL but schema file has REQUIRED"), msg);
  }

  @Test
  public void shouldRejectIncompatibleAnnotations() {
    // string annotation on raw bytes: writer does not guarantee UTF-8
    rejected(schema(NAME, QTY, TOTAL, CREATED, "optional binary raw (STRING);", ID, TAGS));
    // timestamp on a plain BIGINT column
    rejected(schema(NAME, QTY, "optional int64 total (TIMESTAMP(MILLIS,true));", CREATED, RAW,
        ID, TAGS));
    // wrong timestamp unit
    rejected(schema(NAME, QTY, TOTAL, "optional int64 created (TIMESTAMP(MICROS,true));", RAW,
        ID, TAGS));
    // decimal on int32
    rejected(schema(NAME, "optional int32 qty (DECIMAL(9,2));", TOTAL, CREATED, RAW, ID, TAGS));
    // narrower int width
    rejected(schema(NAME, "optional int32 qty (INTEGER(16,true));", TOTAL, CREATED, RAW, ID,
        TAGS));
  }

  @Test
  public void shouldRejectFixedLengthWithoutUuidAnnotation() {
    final String msg =
        rejected(schema(NAME, QTY, TOTAL, CREATED, RAW,
            "optional fixed_len_byte_array(16) id;", TAGS));
    Assertions.assertTrue(msg.contains("id (#6"), msg);
  }

  @Test
  public void shouldRejectListChanges() {
    rejected(schema(NAME, QTY, TOTAL, CREATED, RAW, ID, "optional binary tags;"));
    rejected(schema(NAME, QTY, TOTAL, CREATED, RAW, ID,
        "optional group tags { repeated group list { optional int32 element; } }"));
    final String msg =
        rejected(schema(NAME, QTY, TOTAL, CREATED, RAW, ID,
            "optional group tags (LIST) { repeated group list { optional int64 element; } }"));
    Assertions.assertTrue(msg.contains("tags (#7, JDBC column tags).list (#1"), msg);
  }

  @Test
  public void shouldListAllOffendingColumns() {
    final String msg =
        rejected(schema("optional int32 name;", "optional double qty;", TOTAL, CREATED, RAW, ID,
            TAGS));
    Assertions.assertTrue(msg.contains("name (#1"), msg);
    Assertions.assertTrue(msg.contains("qty (#2"), msg);
  }

  private static final MessageType DERIVED_LOGICAL_MICROS =
      MessageTypeParser.parseMessageType(
          DERIVED_LOGICAL.toString().replace("TIMESTAMP(MILLIS,true)", "TIMESTAMP(MICROS,true)"));

  private static String createdAs(final String created) {
    return schema(NAME, QTY, TOTAL, created, RAW, ID, TAGS);
  }

  private static void validateMicros(final String text) throws FailedValidationException {
    ParquetSchemaValidator.validate(
        MessageTypeParser.parseMessageType(text),
        DERIVED,
        DERIVED_LOGICAL_MICROS,
        DERIVED_LOGICAL_MICROS);
  }

  @Test
  public void shouldAcceptMicrosWithTimestampMicrosFlag() throws Exception {
    validateMicros(createdAs("optional int64 created (TIMESTAMP(MICROS,true));"));
    validateMicros(createdAs(CREATED));
  }

  @Test
  public void shouldRejectMicrosWithoutTimestampMicrosFlag() {
    final String msg = rejected(createdAs("optional int64 created (TIMESTAMP(MICROS,true));"));
    Assertions.assertTrue(msg.contains("--useTimestampMicros is not set"), msg);
  }

  @Test
  public void shouldRejectMillisWithTimestampMicrosFlag() {
    final String msg =
        Assertions.assertThrows(
                FailedValidationException.class,
                () -> validateMicros(createdAs("optional int64 created (TIMESTAMP(MILLIS,true));")))
            .getMessage();
    Assertions.assertTrue(msg.contains("--useTimestampMicros is set"), msg);
    // expected schema in the message reflects the actual flags
    Assertions.assertTrue(msg.contains("created (TIMESTAMP(MICROS,true))"), msg);
  }

  @Test
  public void shouldRejectNanosTimestamps() {
    final String nanos = createdAs("optional int64 created (TIMESTAMP(NANOS,true));");
    Assertions.assertTrue(rejected(nanos).contains("NANOS is not supported"));
    Assertions.assertThrows(FailedValidationException.class, () -> validateMicros(nanos));
  }

  @Test
  public void shouldRejectNonUtcTimestamps() {
    final String msg = rejected(createdAs("optional int64 created (TIMESTAMP(MILLIS,false));"));
    Assertions.assertTrue(msg.contains("isAdjustedToUTC"), msg);
  }
}
