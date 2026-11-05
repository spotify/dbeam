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

import com.spotify.dbeam.args.JdbcExportArgs;
import com.spotify.dbeam.jobs.FailedValidationException;
import com.spotify.dbeam.options.JdbcExportArgsFactory;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates a user-provided Avro schema (via {@code --avroSchemaFilePath}) against what {@link
 * JdbcAvroRecordConverter} writes for the JDBC result set.
 *
 * <p>The writer binary-encodes each column by position, using the JDBC column type (see {@link
 * JdbcAvroRecord#computeMapping}) and always writing union index 0 for NULL and 1 for a value. The
 * provided schema is used verbatim as the writer schema, so it may only rename fields and change
 * docs, namespaces and other properties. For every field:
 *
 * <ul>
 *   <li>the record must have the same number of fields,
 *   <li>the field must be a {@code ["null", T]} union (null first),
 *   <li>{@code T} must have the Avro type the writer emits (e.g. {@code long} for TIMESTAMP, {@code
 *       string} for DECIMAL), recursively for array items, whose nullability must follow {@code
 *       --nullableArrayItems},
 *   <li>a {@code logicalType} must be absent or the one dbeam would generate for the column with
 *       {@code --useLogicalTypes} ({@code timestamp-millis}/{@code timestamp-micros} per {@code
 *       --useTimestampMicros}, {@code uuid}); any other ({@code date}, {@code time-*}, {@code
 *       local-timestamp-*}, {@code decimal}, ...) is rejected.
 * </ul>
 */
public class AvroSchemaValidator {

  private static final Logger LOGGER = LoggerFactory.getLogger(AvroSchemaValidator.class);

  private AvroSchemaValidator() {}

  /**
   * Run a single {@code LIMIT 1} query and validate the provided schema against the schema dbeam
   * derives from its metadata.
   */
  public static void validate(
      final Connection connection, final JdbcExportArgs args, final Schema provided)
      throws SQLException, FailedValidationException {
    JdbcExportArgsFactory.validateInputSchemaTimestampUnits(provided, args.useTimestampMicros());
    try (Statement statement = connection.createStatement()) {
      final ResultSet resultSet =
          statement.executeQuery(args.queryBuilderArgs().sqlQueryWithLimitOne());
      resultSet.next();
      final String url = connection.getMetaData().getURL();
      final Schema derived = derive(resultSet, args, url, args.useAvroLogicalTypes());
      final Schema derivedLogical = derive(resultSet, args, url, true);
      validate(provided, derived, derivedLogical);
      LOGGER.info("--avroSchemaFilePath schema is compatible with the JDBC result set");
    }
  }

  /**
   * Validate the provided schema.
   *
   * @param provided schema read from {@code --avroSchemaFilePath}
   * @param derived schema derived from JDBC metadata with the job's flags
   * @param derivedLogical schema derived with {@code --useLogicalTypes} (and the job's {@code
   *     --useTimestampMicros}), whose logical types are also accepted
   */
  public static void validate(
      final Schema provided, final Schema derived, final Schema derivedLogical)
      throws FailedValidationException {
    final List<String> errors = new ArrayList<>();
    if (provided.getType() != Schema.Type.RECORD) {
      errors.add("<root>: expected a record but schema file has " + provided.getType());
    } else if (provided.getFields().size() != derived.getFields().size()) {
      errors.add(
          String.format(
              "<root>: expected %d fields but schema file has %d",
              derived.getFields().size(), provided.getFields().size()));
    } else {
      for (int i = 0; i < derived.getFields().size(); i++) {
        final Schema.Field p = provided.getFields().get(i);
        final Schema.Field d = derived.getFields().get(i);
        final Schema.Field dl = derivedLogical.getFields().get(i);
        final String path =
            String.format(
                "%s (#%d, JDBC column %s)",
                p.name(), i + 1, Optional.ofNullable(d.getProp("columnName")).orElse(d.name()));
        compareNullable(path, p.schema(), d.schema(), dl.schema(), true, errors);
      }
    }
    if (!errors.isEmpty()) {
      throw new FailedValidationException(
          String.format(
              "--avroSchemaFilePath schema is incompatible with the JDBC result set. Values are"
                  + " written by field position from the JDBC column types, so a schema file may"
                  + " only rename fields and change docs, namespaces or matching logical types."
                  + " Problems:%n  %s%nExpected schema (derived from JDBC):%n%s",
              String.join(String.format("%n  "), errors),
              derived.toString(true)));
    }
  }

  private static Schema derive(
      final ResultSet resultSet,
      final JdbcExportArgs args,
      final String url,
      final boolean useLogicalTypes)
      throws SQLException {
    return JdbcAvroSchema.createAvroSchema(
        resultSet,
        args.avroSchemaNamespace(),
        url,
        Optional.empty(),
        "",
        useLogicalTypes,
        args.useTimestampMicros(),
        args.jdbcAvroOptions().arrayMode(),
        args.jdbcAvroOptions().nullableArrayItems(),
        true);
  }

  /**
   * Compare a union written with {@code nullFirst} ({@code ["null", T]} for fields, {@code [T,
   * "null"]} for nullable array items).
   */
  private static void compareNullable(
      final String path,
      final Schema provided,
      final Schema derived,
      final Schema derivedLogical,
      final boolean nullFirst,
      final List<String> errors) {
    final int nullIndex = nullFirst ? 0 : 1;
    final int valueIndex = 1 - nullIndex;
    if (provided.getType() != Schema.Type.UNION
        || provided.getTypes().size() != 2
        || provided.getTypes().get(nullIndex).getType() != Schema.Type.NULL) {
      errors.add(
          String.format(
              "%s: expected a union %s but schema file has %s (values are written with union"
                  + " index %d for NULL)",
              path, describe(derived), describe(provided), nullIndex));
      return;
    }
    compareValue(
        path,
        provided.getTypes().get(valueIndex),
        derived.getTypes().get(valueIndex),
        derivedLogical.getTypes().get(valueIndex),
        errors);
  }

  private static void compareValue(
      final String path,
      final Schema provided,
      final Schema derived,
      final Schema derivedLogical,
      final List<String> errors) {
    if (provided.getType() != derived.getType()) {
      errors.add(
          String.format(
              "%s: expected %s but schema file has %s",
              path, describe(derived), describe(provided)));
      return;
    }
    if (derived.getType() == Schema.Type.ARRAY) {
      final Schema items = derived.getElementType();
      if (items.getType() == Schema.Type.NULL) {
        // typed_first_row item type unknown (no row or NULL first value): trust the schema file
        return;
      }
      if (items.getType() == Schema.Type.UNION) {
        compareNullable(
            path + "[]",
            provided.getElementType(),
            items,
            derivedLogical.getElementType(),
            false,
            errors);
      } else if (provided.getElementType().getType() == Schema.Type.UNION) {
        errors.add(
            String.format(
                "%s[]: expected non-nullable items %s but schema file has %s (set"
                    + " --nullableArrayItems for nullable items)",
                path, describe(items), describe(provided.getElementType())));
      } else {
        compareValue(
            path + "[]",
            provided.getElementType(),
            items,
            derivedLogical.getElementType(),
            errors);
      }
      return;
    }
    final String logicalType = provided.getProp("logicalType");
    if (logicalType == null
        || logicalType.equals(derived.getProp("logicalType"))
        || logicalType.equals(derivedLogical.getProp("logicalType"))) {
      return;
    }
    final String derivedLogicalType = derivedLogical.getProp("logicalType");
    if (isTimestamp(logicalType) && derivedLogicalType != null && isTimestamp(derivedLogicalType)) {
      // unit mismatch, already reported by validateInputSchemaTimestampUnits
      return;
    }
    errors.add(
        String.format(
            "%s: logicalType %s is not compatible with %s; expected %s",
            path,
            logicalType,
            describe(derived),
            derivedLogicalType == null
                ? "no logicalType"
                : "no logicalType or " + derivedLogicalType));
  }

  private static boolean isTimestamp(final String logicalType) {
    return logicalType.startsWith("timestamp-") || logicalType.startsWith("local-timestamp-");
  }

  private static String describe(final Schema schema) {
    switch (schema.getType()) {
      case UNION:
        final List<String> branches = new ArrayList<>();
        for (Schema branch : schema.getTypes()) {
          branches.add(describe(branch));
        }
        return "[" + String.join(", ", branches) + "]";
      case ARRAY:
        return "array<" + describe(schema.getElementType()) + ">";
      default:
        final String logicalType = schema.getProp("logicalType");
        final String name = schema.getType().getName();
        return Objects.isNull(logicalType) ? name : name + " (" + logicalType + ")";
    }
  }
}
