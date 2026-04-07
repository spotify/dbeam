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
import static java.sql.Types.DATE;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.OTHER;
import static java.sql.Types.REAL;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TIME_WITH_TIMEZONE;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;

import com.spotify.dbeam.args.QueryBuilderArgs;
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
    LOGGER.debug("Creating Parquet schema based on the first read row from the database");
    try (Statement statement = connection.createStatement()) {
      final ResultSet resultSet = statement.executeQuery(queryBuilderArgs.sqlQueryWithLimitOne());
      resultSet.next();
      final MessageType schema =
          createParquetSchema(resultSet, schemaName, useLogicalTypes, arrayMode);
      LOGGER.info(
          "Parquet schema created successfully. useLogicalTypes={}. Generated schema: {}",
          useLogicalTypes,
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
      final String columnClassName = meta.getColumnClassName(i);
      final String columnTypeName = meta.getColumnTypeName(i);

      fields.add(
          buildParquetFieldType(
              normalizeForAvro(columnName),
              columnType,
              precision,
              columnClassName,
              columnTypeName,
              useLogicalTypes,
              arrayMode));
    }

    return new MessageType(schemaName, fields);
  }

  static String getDatabaseTableName(final ResultSetMetaData meta) throws SQLException {
    final String defaultTableName = "no_table_name";
    for (int i = 1; i <= meta.getColumnCount(); i++) {
      String metaTableName = meta.getTableName(i);
      if (metaTableName != null && !metaTableName.isEmpty()) {
        return normalizeForAvro(metaTableName);
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
    switch (columnType) {
      case BIGINT:
        return Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
            .named(columnName);
      case INTEGER:
      case SMALLINT:
      case TINYINT:
        if (Long.class.getCanonicalName().equals(columnClassName)) {
          return Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
              .named(columnName);
        } else {
          return Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
              .named(columnName);
        }
      case TIMESTAMP:
      case DATE:
      case TIME:
      case TIME_WITH_TIMEZONE:
        if (useLogicalTypes) {
          return Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
              .as(LogicalTypeAnnotation.timestampType(true,
                  LogicalTypeAnnotation.TimeUnit.MILLIS))
              .named(columnName);
        } else {
          return Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
              .named(columnName);
        }
      case BOOLEAN:
        return Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN)
            .named(columnName);
      case BIT:
        if (precision <= 1) {
          return Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN)
              .named(columnName);
        } else {
          return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
              .named(columnName);
        }
      case BINARY:
      case VARBINARY:
      case LONGVARBINARY:
      case BLOB:
        return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .named(columnName);
      case DOUBLE:
        return Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named(columnName);
      case FLOAT:
      case REAL:
        return Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT)
            .named(columnName);
      case ARRAY:
        if ("bytes".equals(arrayMode)) {
          return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
              .named(columnName);
        }
        // Parquet 3-level LIST convention with typed elements.
        // Element type is inferred from columnTypeName (e.g. _int4, _text for PostgreSQL).
        return Types.optionalList()
            .element(buildArrayElementType(columnTypeName))
            .named(columnName);
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

  /**
   * Determine the Parquet element type for an ARRAY column based on the column type name.
   * For PostgreSQL, array column type names are prefixed with underscore (e.g. _int4, _text).
   * Falls back to STRING for unrecognized types.
   */
  static Type buildArrayElementType(final String columnTypeName) {
    final String elementType = resolveArrayElementTypeName(columnTypeName);
    switch (elementType) {
      case "int":
      case "int4":
      case "int2":
        return Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
            .named("element");
      case "int8":
        return Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
            .named("element");
      case "float4":
        return Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT)
            .named("element");
      case "float8":
        return Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("element");
      case "bool":
        return Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN)
            .named("element");
      default:
        return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("element");
    }
  }

  /**
   * Extract the element type name from an array column type name.
   * PostgreSQL uses underscore prefix (e.g. _int4 -> int4, _text -> text).
   * H2 and others use "INTEGER ARRAY" style names.
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

  private static String normalizeForAvro(final String input) {
    return input.replaceAll("[^A-Za-z0-9_]", "_");
  }
}
