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

import static java.sql.Types.ARRAY;
import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BIT;
import static java.sql.Types.BLOB;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CHAR;
import static java.sql.Types.CLOB;
import static java.sql.Types.DATALINK;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.OTHER;
import static java.sql.Types.REAL;
import static java.sql.Types.REF;
import static java.sql.Types.REF_CURSOR;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.STRUCT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TIME_WITH_TIMEZONE;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;

import com.spotify.dbeam.args.QueryBuilderArgs;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JdbcParquetSchema {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcParquetSchema.class);

  public static MessageType createSchemaByReadingOneRow(
      final Connection connection,
      final QueryBuilderArgs queryBuilderArgs,
      final Optional<String> schemaName,
      final boolean useLogicalTypes,
      final String arrayMode)
      throws SQLException {
    return createSchemaByReadingOneRow(
        connection, queryBuilderArgs, schemaName, useLogicalTypes, false, arrayMode);
  }

  public static MessageType createSchemaByReadingOneRow(
      final Connection connection,
      final QueryBuilderArgs queryBuilderArgs,
      final Optional<String> schemaName,
      final boolean useLogicalTypes,
      final boolean useTimestampMicros,
      final String arrayMode)
      throws SQLException {
    LOGGER.debug("Creating Parquet schema based on the first read row from the database");
    try (Statement statement = connection.createStatement()) {
      final ResultSet resultSet = statement.executeQuery(queryBuilderArgs.sqlQueryWithLimitOne());
      resultSet.next();
      final MessageType schema =
          createParquetSchema(
              resultSet, schemaName, useLogicalTypes, useTimestampMicros, arrayMode);
      LOGGER.info(
          "Parquet schema created successfully. useLogicalTypes={}, useTimestampMicros={}. "
              + "Generated schema: {}",
          useLogicalTypes,
          useTimestampMicros,
          schema);
      return schema;
    }
  }

  public static MessageType createParquetSchema(
      final ResultSet resultSet,
      final Optional<String> maybeSchemaName,
      final boolean useLogicalTypes,
      final String arrayMode)
      throws SQLException {
    return createParquetSchema(resultSet, maybeSchemaName, useLogicalTypes, false, arrayMode);
  }

  public static MessageType createParquetSchema(
      final ResultSet resultSet,
      final Optional<String> maybeSchemaName,
      final boolean useLogicalTypes,
      final boolean useTimestampMicros,
      final String arrayMode)
      throws SQLException {
    return createParquetSchema(
        resultSet, maybeSchemaName, useLogicalTypes, useTimestampMicros, arrayMode, false);
  }

  /**
   * Creates the Parquet schema for a result set.
   *
   * @param nativeTypes {@code --parquetNativeTypes}: DATE, TIME and DECIMAL/NUMERIC (with a valid
   *     precision) use Parquet's DATE, TIME and DECIMAL types instead of the Avro-compatible
   *     encodings. Annotations are added even without {@code useLogicalTypes}, since the physical
   *     type depends on them (only used that way to validate schema files).
   */
  public static MessageType createParquetSchema(
      final ResultSet resultSet,
      final Optional<String> maybeSchemaName,
      final boolean useLogicalTypes,
      final boolean useTimestampMicros,
      final String arrayMode,
      final boolean nativeTypes)
      throws SQLException {
    final ResultSetMetaData meta = resultSet.getMetaData();
    final String tableName = getDatabaseTableName(meta);
    final String schemaName = maybeSchemaName.orElse(tableName);

    final List<Type> fields = new ArrayList<>();
    for (int i = 1; i <= meta.getColumnCount(); i++) {
      final String columnName;
      if (meta.getColumnName(i).isEmpty()) {
        columnName = meta.getColumnLabel(i);
      } else {
        columnName = meta.getColumnName(i);
      }

      final int columnType = meta.getColumnType(i);
      final int precision = meta.getPrecision(i);
      final int scale = meta.getScale(i);
      final String columnClassName = meta.getColumnClassName(i);
      final String columnTypeName = meta.getColumnTypeName(i);

      fields.add(
          buildParquetFieldType(
              normalizeFieldName(columnName),
              columnType,
              precision,
              scale,
              columnClassName,
              columnTypeName,
              useLogicalTypes,
              useTimestampMicros,
              arrayMode,
              nativeTypes));
    }

    return new MessageType(schemaName, fields);
  }

  static String getDatabaseTableName(final ResultSetMetaData meta) throws SQLException {
    final String defaultTableName = "no_table_name";
    for (int i = 1; i <= meta.getColumnCount(); i++) {
      String metaTableName = meta.getTableName(i);
      if (metaTableName != null && !metaTableName.isEmpty()) {
        return normalizeFieldName(metaTableName);
      }
    }
    return defaultTableName;
  }

  static Type buildParquetFieldType(
      final String columnName,
      final int columnType,
      final int precision,
      final String columnClassName,
      final String columnTypeName,
      final boolean useLogicalTypes,
      final String arrayMode) {
    return buildParquetFieldType(
        columnName,
        columnType,
        precision,
        columnClassName,
        columnTypeName,
        useLogicalTypes,
        false,
        arrayMode);
  }

  static Type buildParquetFieldType(
      final String columnName,
      final int columnType,
      final int precision,
      final String columnClassName,
      final String columnTypeName,
      final boolean useLogicalTypes,
      final boolean useTimestampMicros,
      final String arrayMode) {
    return buildParquetFieldType(
        columnName,
        columnType,
        precision,
        0,
        columnClassName,
        columnTypeName,
        useLogicalTypes,
        useTimestampMicros,
        arrayMode,
        false);
  }

  static Type buildParquetFieldType(
      final String columnName,
      final int columnType,
      final int precision,
      final int scale,
      final String columnClassName,
      final String columnTypeName,
      final boolean useLogicalTypes,
      final boolean useTimestampMicros,
      final String arrayMode,
      final boolean nativeTypes) {
    if (nativeTypes) {
      final Optional<Type> nativeType =
          buildNativeFieldType(columnName, columnType, precision, scale, useTimestampMicros);
      if (nativeType.isPresent()) {
        return nativeType.get();
      }
    }
    switch (columnType) {
      case BIGINT:
        return Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named(columnName);
      case INTEGER:
      case SMALLINT:
      case TINYINT:
        if (Long.class.getCanonicalName().equals(columnClassName)) {
          return Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named(columnName);
        } else {
          return Types.optional(PrimitiveType.PrimitiveTypeName.INT32).named(columnName);
        }
      case TIMESTAMP:
      case DATE: // stored as TIMESTAMP(MILLIS|MICROS) for Avro-path compatibility
      case TIME:
      case TIME_WITH_TIMEZONE:
        if (useLogicalTypes) {
          final LogicalTypeAnnotation.TimeUnit unit =
              useTimestampMicros
                  ? LogicalTypeAnnotation.TimeUnit.MICROS
                  : LogicalTypeAnnotation.TimeUnit.MILLIS;
          return Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
              .as(LogicalTypeAnnotation.timestampType(true, unit))
              .named(columnName);
        } else {
          return Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named(columnName);
        }
      case BOOLEAN:
        return Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named(columnName);
      case BIT:
        if (precision <= 1) {
          return Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named(columnName);
        } else {
          return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).named(columnName);
        }
      case BINARY:
      case VARBINARY:
      case LONGVARBINARY:
      case BLOB:
        return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).named(columnName);
      case DOUBLE:
        return Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named(columnName);
      case FLOAT:
      case REAL:
        return Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT).named(columnName);
      case ARRAY:
        if ("bytes".equals(arrayMode)) {
          return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).named(columnName);
        }
        // Parquet 3-level LIST convention with typed elements.
        // Element type is inferred from columnTypeName (e.g. _int4, _text for PostgreSQL).
        return Types.optionalList()
            .element(buildArrayElementType(columnTypeName))
            .named(columnName);
      // Same as dbeam-core's JdbcAvroSchema
      case STRUCT:
        throw new RuntimeException("STRUCT type is not supported");
      case REF:
      case REF_CURSOR:
        throw new RuntimeException("REF and REF_CURSOR type are not supported");
      case DATALINK:
        throw new RuntimeException("DATALINK type is not supported");
      case OTHER:
        if (useLogicalTypes && "uuid".equals(columnTypeName)) {
          return Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
              .length(16)
              .as(LogicalTypeAnnotation.uuidType())
              .named(columnName);
        }
        // fall through to string
      case VARCHAR:
      case CHAR:
      case CLOB:
      case LONGNVARCHAR:
      case LONGVARCHAR:
      case NCHAR:
      default:
        return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named(columnName);
    }
  }

  /** Largest DECIMAL precision written as a Parquet DECIMAL with {@code --parquetNativeTypes}. */
  static final int MAX_NATIVE_DECIMAL_PRECISION = 38;

  /** Whether a DECIMAL/NUMERIC column is written as Parquet DECIMAL with --parquetNativeTypes. */
  static boolean isNativeDecimal(final int precision, final int scale) {
    return precision >= 1
        && precision <= MAX_NATIVE_DECIMAL_PRECISION
        && scale >= 0
        && scale <= precision;
  }

  /** Smallest FIXED_LEN_BYTE_ARRAY length holding any unscaled value of {@code precision}. */
  static int decimalByteLength(final int precision) {
    final BigInteger max = BigInteger.TEN.pow(precision);
    int bytes = 1;
    while (BigInteger.ONE.shiftLeft(8 * bytes - 1).compareTo(max) < 0) {
      bytes++;
    }
    return bytes;
  }

  /**
   * {@code --parquetNativeTypes} mapping: DATE as {@code int32 (DATE)}, TIME/TIME_WITH_TIMEZONE
   * as {@code TIME(MILLIS|MICROS, true)} ({@code int32} for millis, {@code int64} for micros),
   * DECIMAL/NUMERIC with precision 1..38 and 0 <= scale <= precision as {@code
   * fixed_len_byte_array(n) (DECIMAL(p,s))} with the smallest {@code n} for {@code p}. Empty for
   * other columns, which keep the default mapping.
   */
  static Optional<Type> buildNativeFieldType(
      final String columnName,
      final int columnType,
      final int precision,
      final int scale,
      final boolean useTimestampMicros) {
    switch (columnType) {
      case DATE:
        return Optional.of(
            Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                .as(LogicalTypeAnnotation.dateType())
                .named(columnName));
      case TIME:
      case TIME_WITH_TIMEZONE:
        if (useTimestampMicros) {
          return Optional.of(
              Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                  .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
                  .named(columnName));
        }
        return Optional.of(
            Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                .named(columnName));
      case DECIMAL:
      case NUMERIC:
        if (!isNativeDecimal(precision, scale)) {
          return Optional.empty();
        }
        // Always fixed_len_byte_array (allowed by the Parquet spec for any precision): parquet-avro
        // maps int32/int64 DECIMAL to plain Avro int/long, which would lose the scale in the
        // footer Avro schema used by AvroParquetReader (Scio, Beam ParquetIO).
        return Optional.of(
            Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                .length(decimalByteLength(precision))
                .as(LogicalTypeAnnotation.decimalType(scale, precision))
                .named(columnName));
      default:
        return Optional.empty();
    }
  }

  /**
   * Determine the Parquet element type for an ARRAY column based on the column type name. For
   * PostgreSQL, array column type names are prefixed with underscore (e.g. _int4, _text). Falls
   * back to STRING for unrecognized types.
   */
  static Type buildArrayElementType(final String columnTypeName) {
    final String elementType = resolveArrayElementTypeName(columnTypeName);
    switch (elementType) {
      case "int":
      case "int4":
      case "int2":
        return Types.optional(PrimitiveType.PrimitiveTypeName.INT32).named("element");
      case "int8":
        return Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("element");
      case "float4":
        return Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT).named("element");
      case "float8":
        return Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named("element");
      case "bool":
        return Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named("element");
      case "bytea":
        return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).named("element");
      default:
        return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("element");
    }
  }

  /**
   * Extract the element type name from an array column type name. PostgreSQL uses underscore prefix
   * (e.g. _int4 -> int4, _text -> text). H2 and others use "INTEGER ARRAY" style names.
   */
  static String resolveArrayElementTypeName(final String columnTypeName) {
    if (columnTypeName == null) {
      return "text";
    }
    if (columnTypeName.startsWith("_")) {
      return columnTypeName.substring(1);
    }
    // H2 style: "INTEGER ARRAY", "VARCHAR ARRAY", etc.
    final String lower = columnTypeName.toLowerCase();
    if (lower.startsWith("integer")) {
      return "int4";
    } else if (lower.startsWith("bigint")) {
      return "int8";
    } else if (lower.startsWith("real") || lower.startsWith("float")) {
      return "float4";
    } else if (lower.startsWith("double")) {
      return "float8";
    } else if (lower.startsWith("boolean")) {
      return "bool";
    }
    return "text";
  }

  static String normalizeFieldName(final String input) {
    return input.replaceAll("[^A-Za-z0-9_]", "_");
  }
}
