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
import static java.sql.Types.DATE;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
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
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcAvroSchema {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcAvroSchema.class);

  public static Schema createSchemaByReadingOneRow(
      final Connection connection,
      final QueryBuilderArgs queryBuilderArgs,
      final boolean useLogicalTypes,
      final AvroSchemaMetadataProvider provider)
      throws SQLException {
    LOGGER.debug("Creating Avro schema based on the first read row from the database");
    try (Statement statement = connection.createStatement()) {
      final ResultSet resultSet = statement.executeQuery(queryBuilderArgs.sqlQueryWithLimitOne());

      final Schema schema =
          createAvroSchema(resultSet, connection.getMetaData().getURL(), useLogicalTypes, provider);
      LOGGER.info("Schema created successfully. Generated schema: {}", schema.toString());
      return schema;
    }
  }

  public static Schema createAvroSchema(
      final ResultSet resultSet,
      final String connectionUrl,
      final boolean useLogicalTypes,
      final AvroSchemaMetadataProvider provider)
      throws SQLException {

    final ResultSetMetaData meta = resultSet.getMetaData();
    final String tableName = getDatabaseTableName(meta);
    final String recordName = provider.avroSchemaName(tableName);
    final String namespace = provider.avroSchemaNamespace();
    final String recordDoc =
        provider.avroDoc(
            String.format("Generate schema from JDBC ResultSet from %s", connectionUrl));

    final SchemaBuilder.FieldAssembler<Schema> builder =
        SchemaBuilder.record(recordName)
            .namespace(namespace)
            .doc(recordDoc)
            .prop("tableName", tableName)
            .prop("connectionUrl", connectionUrl)
            .fields();
    return createAvroFields(meta, builder, useLogicalTypes, provider).endRecord();
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
      final ResultSetMetaData meta,
      final SchemaBuilder.FieldAssembler<Schema> builder,
      final boolean useLogicalTypes,
      final AvroSchemaMetadataProvider provider)
      throws SQLException {

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
      final SchemaBuilder.FieldBuilder<Schema> field =
          builder
              .name(normalizeForAvro(columnName))
              .doc(
                  provider.getFieldDoc(
                      columnName, String.format("From sqlType %d %s (%s)", columnType, typeName, columnClassName)))
              .prop("columnName", columnName)
              .prop("sqlCode", String.valueOf(columnType))
              .prop("typeName", typeName)
              .prop("columnClassName", columnClassName);

      final SchemaBuilder.BaseTypeBuilder<
              SchemaBuilder.UnionAccumulator<SchemaBuilder.NullDefault<Schema>>>
          fieldSchemaBuilder = field.type().unionOf().nullBuilder().endNull().and();

      final SchemaBuilder.UnionAccumulator<SchemaBuilder.NullDefault<Schema>> schemaFieldAssembler =
          setAvroColumnType(
              columnType,
              meta.getPrecision(i),
              columnClassName,
              useLogicalTypes,
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
   *   <li>{@link org.h2.value.TypeInfo }
   * </ul>
   *
   */
  private static SchemaBuilder.UnionAccumulator<SchemaBuilder.NullDefault<Schema>>
      setAvroColumnType(
          final int columnType,
          final int precision,
          final String columnClassName,
          final boolean useLogicalTypes,
          final SchemaBuilder.BaseTypeBuilder<
                  SchemaBuilder.UnionAccumulator<SchemaBuilder.NullDefault<Schema>>>
              field) {
    switch (columnType) {
      case VARCHAR:
      case CHAR:
      case CLOB:
      case LONGNVARCHAR:
      case LONGVARCHAR:
      case NCHAR:
        return field.stringType();
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
      case BINARY:
      case VARBINARY:
      case LONGVARBINARY:
      case ARRAY:
      case BLOB:
        return field.bytesType();
      case DOUBLE:
        return field.doubleType();
      case FLOAT:
      case REAL:
        return field.floatType();
      default:
        return field.stringType();
    }
  }

  private static String normalizeForAvro(final String input) {
    return input.replaceAll("[^A-Za-z0-9_]", "_");
  }
}
