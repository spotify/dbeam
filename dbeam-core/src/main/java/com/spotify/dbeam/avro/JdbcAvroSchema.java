/*-
 * -\-\-
 * DBeam Core
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
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
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
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
import com.spotify.dbeam.options.ArrayHandlingMode;
import java.sql.Array;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcAvroSchema {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcAvroSchema.class);

  public static Schema createSchemaByReadingOneRow(
      final Connection connection,
      final QueryBuilderArgs queryBuilderArgs,
      final String avroSchemaNamespace,
      final Optional<String> schemaName,
      final String avroDoc,
      final boolean useLogicalTypes,
      final String arrayMode,
      final boolean nullableArrayItems)
      throws SQLException {
    LOGGER.debug("Creating Avro schema based on the first read row from the database");
    try (Statement statement = connection.createStatement()) {
      final ResultSet resultSet = statement.executeQuery(queryBuilderArgs.sqlQueryWithLimitOne());

      if (!resultSet.isBeforeFirst()) {
        LOGGER.warn("When attempting to derive AVRO schema DB query returned no results");
      }

      resultSet.next();

      final Schema schema =
          createAvroSchema(
              resultSet,
              avroSchemaNamespace,
              connection.getMetaData().getURL(),
              schemaName,
              avroDoc,
              useLogicalTypes,
              arrayMode,
              nullableArrayItems);
      LOGGER.info("Schema created successfully. useLogicalTypes={}, arrayMode={}, "
                  + "Generated schema: {}", useLogicalTypes, arrayMode, schema.toString());
      return schema;
    }
  }

  public static Schema createAvroSchema(
      final ResultSet resultSet,
      final String avroSchemaNamespace,
      final String connectionUrl,
      final Optional<String> maybeSchemaName,
      final String avroDoc,
      final boolean useLogicalTypes,
      final String arrayMode,
      final boolean nullableArrayItems)
      throws SQLException {

    final ResultSetMetaData meta = resultSet.getMetaData();
    final String tableName = getDatabaseTableName(meta);
    final String schemaName = maybeSchemaName.orElse(tableName);

    final SchemaBuilder.FieldAssembler<Schema> builder =
        SchemaBuilder.record(schemaName)
            .namespace(avroSchemaNamespace)
            .doc(avroDoc)
            .prop("tableName", tableName)
            .prop("connectionUrl", connectionUrl)
            .fields();
    return createAvroFields(resultSet, builder, useLogicalTypes, arrayMode, nullableArrayItems)
        .endRecord();
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

  private static SchemaBuilder.FieldAssembler<Schema> createAvroFields(
      final ResultSet resultSet,
      final SchemaBuilder.FieldAssembler<Schema> builder,
      final boolean useLogicalTypes,
      final String arrayMode,
      final boolean nullableArrayItems)
      throws SQLException {

    ResultSetMetaData meta = resultSet.getMetaData();

    for (int i = 1; i <= meta.getColumnCount(); i++) {

      final String columnName;
      if (meta.getColumnName(i).isEmpty()) {
        columnName = meta.getColumnLabel(i);
      } else {
        columnName = meta.getColumnName(i);
      }

      final int columnType = meta.getColumnType(i);
      final String typeName = JDBCType.valueOf(columnType).getName();
      final String columnClassName = meta.getColumnClassName(i);
      final String columnTypeName = meta.getColumnTypeName(i);
      SchemaBuilder.FieldBuilder<Schema> field =
          builder
              .name(normalizeForAvro(columnName))
              .doc(String.format("From sqlType %d %s (%s)", columnType, typeName, columnClassName))
              .prop("columnName", columnName)
              .prop("sqlCode", String.valueOf(columnType))
              .prop("typeName", typeName)
              .prop("columnClassName", columnClassName);

      if (columnTypeName != null) {
        field = field.prop("columnTypeName", columnTypeName);
      }

      final SchemaBuilder.BaseTypeBuilder<
              SchemaBuilder.UnionAccumulator<SchemaBuilder.NullDefault<Schema>>>
          fieldSchemaBuilder = field.type().unionOf().nullBuilder().endNull().and();

      Array arrayInstance =
          resultSet.isFirst() && columnType == ARRAY
          && arrayMode.equals(ArrayHandlingMode.TypedMetaFromFirstRow)
          ? resultSet.getArray(i) : null;

      final SchemaBuilder.UnionAccumulator<SchemaBuilder.NullDefault<Schema>> schemaFieldAssembler =
          buildAvroFieldType(
              columnName,
              columnType,
              arrayInstance,
              meta.getPrecision(i),
              columnClassName,
              columnTypeName,
              useLogicalTypes,
              arrayMode,
              nullableArrayItems,
              fieldSchemaBuilder);

      schemaFieldAssembler.endUnion().nullDefault();
    }
    return builder;
  }

  /**
   * Creates Avro field schema based on JDBC MetaData
   *
   * <p>For database specific types implementation, check the following:
   *
   * <ul>
   *   <li>{@link org.postgresql.jdbc.TypeInfoCache }
   *   <li>{@link com.mysql.cj.MysqlType }
   *   <li>org.h2.value.Value
   * </ul>
   */
  private static SchemaBuilder.UnionAccumulator<SchemaBuilder.NullDefault<Schema>>
      buildAvroFieldType(
      final String columnName,
      final int columnType,
      final Array arrayInstance,
      final int precision,
      final String columnClassName,
      final String columnTypeName,
      final boolean useLogicalTypes,
      final String arrayMode,
      final boolean nullableArrayItems,
      final SchemaBuilder.BaseTypeBuilder<SchemaBuilder.UnionAccumulator<
          SchemaBuilder.NullDefault<Schema>>> field) throws SQLException {
    switch (columnType) {
      case BIGINT:
        return field.longType();
      case INTEGER:
      case SMALLINT:
      case TINYINT:
        if (Long.class.getCanonicalName().equals(columnClassName)) {
          return field.longType();
        } else {
          return field.intType();
        }
      case TIMESTAMP:
      case DATE:
      case TIME:
      case TIME_WITH_TIMEZONE:
        if (useLogicalTypes) {
          return field.longBuilder().prop("logicalType", "timestamp-millis").endLong();
        } else {
          return field.longType();
        }
      case BOOLEAN:
        return field.booleanType();
      case BIT:
        // Note that bit types can take a param/typemod qualifying its length
        // some further docs:
        // https://www.postgresql.org/docs/8.2/datatype-bit.html
        if (precision <= 1) {
          return field.booleanType();
        } else {
          return field.bytesType();
        }
      case ARRAY:
        if (arrayMode.equals(ArrayHandlingMode.Bytes)) {
          return field.bytesType();
        }

        if (arrayMode.equals(ArrayHandlingMode.TypedMetaPostgres)) {
          if (!columnTypeName.startsWith("_")) {
            throw new RuntimeException("columnName=" + columnName
                                       + " columnTypeName=" + columnTypeName
                                       + " should start with '_'");
          }

          return buildAvroFieldTypeFromPGType(columnTypeName.substring(1),
              useLogicalTypes, buildArrayItems(nullableArrayItems, field));
        }

        if (arrayInstance == null) {
          throw new RuntimeException(
              "When inspecting ARRAY column type in '" + columnName
              + "' its first value is NULL");
        }
        return buildAvroFieldType(
            columnName,
            arrayInstance.getBaseType(),
            null,
            precision,
            columnClassName,
            arrayInstance.getBaseTypeName(),
            useLogicalTypes,
            arrayMode,
            nullableArrayItems,
            buildArrayItems(nullableArrayItems, field));
      case BINARY:
      case VARBINARY:
      case LONGVARBINARY:
      case BLOB:
        return field.bytesType();
      case DOUBLE:
        return field.doubleType();
      case FLOAT:
      case REAL:
        return field.floatType();
      case OTHER:
        if (useLogicalTypes && Objects.equals(columnTypeName, "uuid")) {
          return field.stringBuilder().prop("logicalType", "uuid").endString();
        } else {
          return field.stringType();
        }
      case STRUCT:
        throw new RuntimeException("STRUCT type is not supported");
      case REF:
      case REF_CURSOR:
        throw new RuntimeException("REF and REF_CURSOR type are not supported");
      case DATALINK:
        throw new RuntimeException("DATALINK type is not supported");
      case VARCHAR:
      case CHAR:
      case CLOB:
      case LONGNVARCHAR:
      case LONGVARCHAR:
      case NCHAR:
      default:
        return field.stringType();
    }
  }

  private static SchemaBuilder.BaseTypeBuilder<SchemaBuilder.UnionAccumulator<
      SchemaBuilder.NullDefault<Schema>>>
      buildArrayItems(
      final boolean nullableArrayItems,
      final SchemaBuilder.BaseTypeBuilder<SchemaBuilder.UnionAccumulator<
          SchemaBuilder.NullDefault<Schema>>> field) {
    if (nullableArrayItems) {
      return field.array().items().nullable();
    } else {
      return field.array().items();
    }
  }

  private static SchemaBuilder.UnionAccumulator<SchemaBuilder.NullDefault<Schema>>
      buildAvroFieldTypeFromPGType(
      final String columnTypeName,
      final boolean useLogicalTypes,
      final SchemaBuilder.BaseTypeBuilder<
          SchemaBuilder.UnionAccumulator<SchemaBuilder.NullDefault<Schema>>>
          field) {
    switch (columnTypeName) {
      case "uuid":
        if (useLogicalTypes) {
          return field.stringBuilder().prop("logicalType", "uuid").endString();
        } else {
          return field.stringType();
        }
      case "int":
      case "int4":
        return field.intType();
      case "int8":
        return field.longType();
      case "varchar":
      case "char":
      case "text":
        return field.stringType();
      default:
        throw new RuntimeException("Postgres type '" + columnTypeName + "' is not supported");
    }
  }

  private static String normalizeForAvro(final String input) {
    return input.replaceAll("[^A-Za-z0-9_]", "_");
  }
}
