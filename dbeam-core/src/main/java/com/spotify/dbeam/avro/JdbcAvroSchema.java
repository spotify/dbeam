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

import com.spotify.dbeam.args.JdbcExportArgs;
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

  private static Logger logger = LoggerFactory.getLogger(JdbcAvroSchema.class);

  public static Schema createSchemaByReadingOneRow(
      Connection connection,
      JdbcExportArgs jdbcExportArgs,
      String avroDoc) throws SQLException {

    String fullTableName;
    String singleRowQuery;

    System.out.println(connection.getMetaData().getURL());
    if (jdbcExportArgs.queryBuilderArgs().tableSchema().isPresent()) {
      fullTableName = jdbcExportArgs.queryBuilderArgs().tableSchema().get()
          + "." + jdbcExportArgs.queryBuilderArgs().tableName();
    } else {
      fullTableName = jdbcExportArgs.queryBuilderArgs().tableName();
    }

    logger.debug("Creating Avro schema based on the first read row from %s", fullTableName);

    try (Statement statement = connection.createStatement()) {
      if (connection.getMetaData().getURL().contains("oracle")) {
        singleRowQuery = String.format("SELECT * FROM %s WHERE ROWNUM = 1",
            fullTableName);
      } else {
        singleRowQuery = String.format("SELECT * FROM %s LIMIT 1",
            fullTableName);
      }

      final ResultSet
          resultSet =
          statement.executeQuery(singleRowQuery);

      System.out.println(connection.getMetaData().getURL());
      System.out.println(jdbcExportArgs.queryBuilderArgs().tableName());
      System.out.println(jdbcExportArgs.queryBuilderArgs().tableSchema().get());
      System.out.println(jdbcExportArgs.avroSchemaNamespace());
      System.out.println(avroDoc);
      System.out.println(jdbcExportArgs.useAvroLogicalTypes().toString());

      Schema schema = JdbcAvroSchema.createAvroSchema(
          resultSet,
          fullTableName,
          jdbcExportArgs.avroSchemaNamespace(),
          connection.getMetaData().getURL(),
          avroDoc,
          jdbcExportArgs.useAvroLogicalTypes());
      logger.info("Schema created successfully. Generated schema: {}", schema.toString());
      return schema;
    }
  }

  private static void printResultSet(ResultSet resultSet) throws SQLException {
    ResultSetMetaData meta = resultSet.getMetaData();
    int columnsNumber = meta.getColumnCount();
    while (resultSet.next()) {
      for (int i = 1; i <= columnsNumber; i++) {
        if (i > 1) {
          System.out.print(", ");
        }
        String columnValue = resultSet.getString(i);
        System.out.println(columnValue);
        System.out.println(meta.getColumnName(i));
        System.out.println(meta.getColumnTypeName(i)
            + ": " + JDBCType.valueOf(meta.getColumnType(i)).toString());
        System.out.println(meta.getColumnType(i));
      }
      System.out.println(" ");
    }
  }

  public static Schema createAvroSchema(
      ResultSet resultSet,
      String fullTableName,
      String avroSchemaNamespace,
      String connectionUrl,
      String avroDoc,
      boolean useLogicalTypes) throws SQLException {
    ResultSetMetaData meta = resultSet.getMetaData();
    String tableName;


    if (meta.getColumnCount() > 0 && !meta.getTableName(1).isEmpty()) {
      tableName = normalizeForAvro(meta.getTableName(1));
    } else {
      tableName = fullTableName;
    }

    SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record(tableName)
        .namespace(avroSchemaNamespace)
        .doc(avroDoc)
        .prop("tableName", normalizeForAvro(fullTableName.toLowerCase()))
        .prop("connectionUrl", connectionUrl)
        .fields();
    return createAvroFields(meta, builder, useLogicalTypes).endRecord();
  }

  private static SchemaBuilder.FieldAssembler<Schema> createAvroFields(
      ResultSetMetaData meta,
      SchemaBuilder.FieldAssembler<Schema> builder,
      boolean useLogicalTypes) throws SQLException {
    for (int i = 1; i <= meta.getColumnCount(); i++) {

      String columnName;
      if (meta.getColumnName(i).isEmpty()) {
        columnName = meta.getColumnLabel(i);
      } else {
        columnName = meta.getColumnName(i);
      }

      int columnType = meta.getColumnType(i);
      String typeName = JDBCType.valueOf(columnType).getName();

      SchemaBuilder.FieldBuilder<Schema> field = builder
          .name(normalizeForAvro(columnName))
          .doc(String.format("From sqlType %d %s", columnType, typeName))
          .prop("columnName", columnName)
          .prop("sqlCode", String.valueOf(columnType))
          .prop("typeName", typeName);
      fieldAvroType(columnType, meta.getPrecision(i), field, useLogicalTypes);
    }
    return builder;
  }

  private static SchemaBuilder.FieldAssembler<Schema> fieldAvroType(
      int columnType,
      int precision,
      SchemaBuilder.FieldBuilder<Schema> fieldBuilder,
      boolean useLogicalTypes) {

    final SchemaBuilder.BaseTypeBuilder<
        SchemaBuilder.UnionAccumulator<SchemaBuilder.NullDefault<Schema>>>
        field =
        fieldBuilder.type().unionOf().nullBuilder().endNull().and();

    switch (columnType) {
      case VARCHAR:
      case CHAR:
      case CLOB:
      case LONGNVARCHAR:
      case LONGVARCHAR:
      case NCHAR:
        return field.stringType().endUnion().nullDefault();
      case BIGINT:
        if (precision > 0 && precision <= JdbcAvroRecord.MAX_DIGITS_BIGINT) {
          return field.longType().endUnion().nullDefault();
        } else {
          return field.stringType().endUnion().nullDefault();
        }
      case INTEGER:
      case SMALLINT:
      case TINYINT:
        return field.intType().endUnion().nullDefault();
      case TIMESTAMP:
      case DATE:
      case TIME:
      case TIME_WITH_TIMEZONE:
        if (useLogicalTypes) {
          return field.longBuilder().prop("logicalType", "timestamp-millis")
              .endLong().endUnion().nullDefault();
        } else {
          return field.longType().endUnion().nullDefault();
        }
      case BOOLEAN:
        return field.booleanType().endUnion().nullDefault();
      case BIT:
        if (precision <= 1) {
          return field.booleanType().endUnion().nullDefault();
        } else {
          return field.bytesType().endUnion().nullDefault();
        }
      case BINARY:
      case VARBINARY:
      case LONGVARBINARY:
      case ARRAY:
      case BLOB:
        return field.bytesType().endUnion().nullDefault();
      case DOUBLE:
        return field.doubleType().endUnion().nullDefault();
      case FLOAT:
      case REAL:
        return field.floatType().endUnion().nullDefault();
      default:
        return field.stringType().endUnion().nullDefault();
    }
  }

  private static String normalizeForAvro(String input) {
    return input.replaceAll("[^A-Za-z0-9_]", "_");
  }

}
