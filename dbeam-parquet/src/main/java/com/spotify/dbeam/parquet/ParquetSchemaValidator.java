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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

/**
 * Validates a user-provided Parquet schema (via {@code --parquetSchemaFilePath}) against the schema
 * derived from JDBC metadata.
 *
 * <p>The writers always take column types from JDBC and write fields by position, so a schema file
 * may only rename fields and change annotations in ways that do not change how the written bytes
 * are interpreted. For every field (recursively for groups) the following must match the derived
 * schema exactly: field count, primitive vs group, primitive type, type length, and repetition. The
 * logical type annotation must be one of:
 *
 * <ul>
 *   <li>the annotation of the derived field (with or without {@code --useAvroLogicalTypes}),
 *   <li>no annotation, except for UUID and LIST, which change what the writer emits,
 *   <li>{@code ENUM} or {@code JSON} where the derived field is {@code STRING},
 *   <li>a signed {@code INT(32)} on {@code int32} or {@code INT(64)} on {@code int64}.
 * </ul>
 *
 * <p>Timestamp/date/time columns: the writer takes the unit from the target field's annotation, so
 * the only accepted TIMESTAMP annotation is the one {@code --useTimestampMicros} asks for: {@code
 * TIMESTAMP(MICROS,true)} with the flag, {@code TIMESTAMP(MILLIS,true)} without it (no annotation
 * is also accepted and is written as millis). {@code NANOS} is never accepted.
 *
 * <p>A PostgreSQL {@code uuid} column may be either {@code binary (STRING)} or {@code
 * fixed_len_byte_array(16) (UUID)} regardless of {@code --useAvroLogicalTypes}, since the writer
 * follows the schema's annotation for that column.
 */
public class ParquetSchemaValidator {

  private ParquetSchemaValidator() {}

  /**
   * Validate the provided schema.
   *
   * @param provided schema read from {@code --parquetSchemaFilePath}
   * @param derived schema derived from JDBC metadata without logical types
   * @param derivedLogical schema derived from JDBC metadata with logical types, using the job's
   *     {@code --useTimestampMicros} value
   */
  public static void validate(
      final MessageType provided, final MessageType derived, final MessageType derivedLogical)
      throws FailedValidationException {
    validate(provided, derived, derivedLogical, derived);
  }

  /**
   * Validate the provided schema.
   *
   * @param provided schema read from {@code --parquetSchemaFilePath}
   * @param derived schema derived from JDBC metadata without logical types
   * @param derivedLogical schema derived from JDBC metadata with logical types, using the job's
   *     {@code --useTimestampMicros} value
   * @param expected schema derived with the job's actual flags, shown in the error message
   */
  public static void validate(
      final MessageType provided,
      final MessageType derived,
      final MessageType derivedLogical,
      final MessageType expected)
      throws FailedValidationException {
    final List<String> errors = new ArrayList<>();
    compareGroup("", provided, derived, derivedLogical, errors);
    if (!errors.isEmpty()) {
      throw new FailedValidationException(
          String.format(
              "--parquetSchemaFilePath schema is incompatible with the JDBC result set. A schema"
                  + " file may only rename fields or change compatible annotations; physical"
                  + " types, type lengths and repetition must match. Problems:%n  %s%n"
                  + "Expected schema (derived from JDBC):%n%s",
              String.join(String.format("%n  "), errors),
              expected));
    }
  }

  private static void compareGroup(
      final String path,
      final GroupType provided,
      final GroupType derived,
      final GroupType derivedLogical,
      final List<String> errors) {
    if (provided.getFieldCount() != derived.getFieldCount()) {
      errors.add(
          String.format(
              "%s: expected %d fields but schema file has %d",
              path.isEmpty() ? "<root>" : path,
              derived.getFieldCount(),
              provided.getFieldCount()));
      return;
    }
    for (int i = 0; i < derived.getFieldCount(); i++) {
      final Type p = provided.getType(i);
      final Type d = derived.getType(i);
      final Type dl = derivedLogical.getType(i);
      final String fieldPath =
          (path.isEmpty() ? "" : path + ".")
              + String.format("%s (#%d, JDBC column %s)", p.getName(), i + 1, d.getName());
      compareField(fieldPath, p, d, dl, errors);
    }
  }

  private static void compareField(
      final String path,
      final Type provided,
      final Type derived,
      final Type derivedLogical,
      final List<String> errors) {
    // PostgreSQL uuid: the writer follows the schema annotation, accept either form.
    if (isUuid(derivedLogical) && isUuid(provided)) {
      compareRepetition(path, provided, derived, errors);
      return;
    }
    if (provided.isPrimitive() != derived.isPrimitive()) {
      errors.add(
          String.format(
              "%s: expected %s but schema file has %s",
              path, describe(derived), describe(provided)));
      return;
    }
    compareRepetition(path, provided, derived, errors);
    final LogicalTypeAnnotation pa = provided.getLogicalTypeAnnotation();
    if (provided.isPrimitive()) {
      final PrimitiveType pp = provided.asPrimitiveType();
      final PrimitiveType dp = derived.asPrimitiveType();
      if (pp.getPrimitiveTypeName() != dp.getPrimitiveTypeName()
          || pp.getTypeLength() != dp.getTypeLength()) {
        errors.add(
            String.format(
                "%s: expected %s but schema file has %s",
                path, describe(derived), describe(provided)));
        return;
      }
      // Only consider the logical-types annotation when it does not change the physical type.
      final LogicalTypeAnnotation logical =
          derivedLogical.isPrimitive()
                  && derivedLogical.asPrimitiveType().getPrimitiveTypeName()
                      == dp.getPrimitiveTypeName()
                  && derivedLogical.asPrimitiveType().getTypeLength() == dp.getTypeLength()
              ? derivedLogical.getLogicalTypeAnnotation()
              : dp.getLogicalTypeAnnotation();
      if (pa instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
          && logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
          && !pa.equals(logical)) {
        errors.add(timestampError(path, pa, logical));
      } else if (!isCompatibleAnnotation(pa, dp, logical)) {
        errors.add(
            String.format(
                "%s: annotation %s is not compatible with %s", path, pa, describe(derived)));
      }
    } else {
      if (!Objects.equals(pa, derived.getLogicalTypeAnnotation())) {
        errors.add(
            String.format(
                "%s: group annotation must be %s but schema file has %s",
                path, derived.getLogicalTypeAnnotation(), pa));
        return;
      }
      compareGroup(
          path,
          provided.asGroupType(),
          derived.asGroupType(),
          derivedLogical.asGroupType(),
          errors);
    }
  }

  private static void compareRepetition(
      final String path, final Type provided, final Type derived, final List<String> errors) {
    if (provided.getRepetition() != derived.getRepetition()) {
      errors.add(
          String.format(
              "%s: expected repetition %s but schema file has %s",
              path, derived.getRepetition(), provided.getRepetition()));
    }
  }

  static boolean isCompatibleAnnotation(
      final LogicalTypeAnnotation provided,
      final PrimitiveType derived,
      final LogicalTypeAnnotation derivedLogical) {
    final LogicalTypeAnnotation derivedAnnotation = derived.getLogicalTypeAnnotation();
    if (provided == null
        || provided.equals(derivedAnnotation)
        || provided.equals(derivedLogical)) {
      return true;
    }
    final PrimitiveTypeName name = derived.getPrimitiveTypeName();
    if (LogicalTypeAnnotation.stringType().equals(derivedAnnotation)) {
      return provided.equals(LogicalTypeAnnotation.enumType())
          || provided.equals(LogicalTypeAnnotation.jsonType());
    }
    if (derivedAnnotation == null && name == PrimitiveTypeName.INT32) {
      return provided.equals(LogicalTypeAnnotation.intType(32, true));
    }
    if (derivedAnnotation == null
        && derivedLogical == null
        && name == PrimitiveTypeName.INT64) {
      return provided.equals(LogicalTypeAnnotation.intType(64, true));
    }
    return false;
  }

  private static String timestampError(
      final String path,
      final LogicalTypeAnnotation provided,
      final LogicalTypeAnnotation logical) {
    final boolean micros =
        ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logical).getUnit()
            == LogicalTypeAnnotation.TimeUnit.MICROS;
    final LogicalTypeAnnotation.TimeUnit providedUnit =
        ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) provided).getUnit();
    final String hint;
    if (providedUnit == LogicalTypeAnnotation.TimeUnit.NANOS) {
      hint = "NANOS is not supported";
    } else if (providedUnit
        == ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logical).getUnit()) {
      hint = "only UTC-adjusted (isAdjustedToUTC=true) timestamps are supported";
    } else {
      hint =
          micros
              ? "--useTimestampMicros is set; use TIMESTAMP(MICROS,true) or remove the flag"
              : "--useTimestampMicros is not set; use TIMESTAMP(MILLIS,true) or set the flag";
    }
    return String.format(
        "%s: annotation %s is not compatible, expected %s (%s)", path, provided, logical, hint);
  }

  private static boolean isUuid(final Type type) {
    return type.isPrimitive()
        && type.asPrimitiveType().getPrimitiveTypeName()
            == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
        && type.asPrimitiveType().getTypeLength() == 16
        && LogicalTypeAnnotation.uuidType().equals(type.getLogicalTypeAnnotation());
  }

  private static String describe(final Type type) {
    if (!type.isPrimitive()) {
      return "group" + (type.getLogicalTypeAnnotation() == null
          ? "" : " (" + type.getLogicalTypeAnnotation() + ")");
    }
    final PrimitiveType p = type.asPrimitiveType();
    final StringBuilder sb = new StringBuilder(p.getPrimitiveTypeName().name().toLowerCase());
    if (p.getPrimitiveTypeName() == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
      sb.append('(').append(p.getTypeLength()).append(')');
    }
    if (p.getLogicalTypeAnnotation() != null) {
      sb.append(" (").append(p.getLogicalTypeAnnotation()).append(')');
    }
    return sb.toString();
  }
}
