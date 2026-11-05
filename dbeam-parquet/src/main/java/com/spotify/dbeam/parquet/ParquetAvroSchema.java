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

import com.spotify.dbeam.avro.JdbcAvroSchema;
import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;

/**
 * Derives the Avro schema stored in the {@code parquet.avro.schema} footer key (and in {@code
 * _AVRO_SCHEMA.avsc}) from the Parquet {@link MessageType} that is actually used for writing, so
 * the two can never contradict each other.
 *
 * <p>The conversion is parquet-avro's {@link AvroSchemaConverter} with {@code
 * parquet.avro.add-list-element-records=false}, so 3-level lists become {@code array<T>} (or
 * {@code array<["null", T]>} for optional elements) without a wrapper record. Only the top-level
 * record is adjusted afterwards: its name, namespace and doc, plus (when JDBC metadata is given)
 * the same record and field properties and docs that dbeam's Avro export writes.
 */
public final class ParquetAvroSchema {

  private ParquetAvroSchema() {}

  /**
   * Converts {@code messageType} to an Avro record named after the message.
   *
   * @throws IllegalArgumentException for Parquet types without an Avro equivalent (e.g. INT96)
   * @throws org.apache.avro.SchemaParseException for names that are not valid Avro names
   */
  public static Schema toAvroSchema(
      final MessageType messageType, final String namespace, final String doc) {
    final Configuration conf = new Configuration(false);
    conf.setBoolean(AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS, false);
    final Schema converted = new AvroSchemaConverter(conf).convert(messageType);
    final List<Schema.Field> fields = new ArrayList<>();
    for (final Schema.Field field : converted.getFields()) {
      fields.add(new Schema.Field(field, field.schema()));
    }
    final Schema record = Schema.createRecord(messageType.getName(), doc, namespace, false);
    record.setFields(fields);
    return record;
  }

  /**
   * Like {@link #toAvroSchema(MessageType, String, String)}, and also adds the {@code tableName}
   * and {@code connectionUrl} record properties and each field's doc and {@code columnName},
   * {@code sqlCode}, {@code typeName}, {@code columnClassName} and {@code columnTypeName}
   * properties, as in {@link JdbcAvroSchema}. Fields are matched to columns by position, so names
   * from a {@code --parquetSchemaFilePath} schema (already validated to have the same columns in
   * the same order) are kept.
   */
  public static Schema toAvroSchema(
      final MessageType messageType,
      final String namespace,
      final String doc,
      final ResultSetMetaData meta,
      final String connectionUrl)
      throws SQLException {
    final Schema converted = toAvroSchema(messageType, namespace, doc);
    final List<Schema.Field> convertedFields = converted.getFields();
    if (convertedFields.size() != meta.getColumnCount()) {
      throw new IllegalArgumentException(
          "Parquet schema has " + convertedFields.size() + " fields but the query returns "
              + meta.getColumnCount() + " columns");
    }
    final List<Schema.Field> fields = new ArrayList<>();
    for (int i = 1; i <= meta.getColumnCount(); i++) {
      final Schema.Field source = convertedFields.get(i - 1);
      final String columnName =
          meta.getColumnName(i).isEmpty() ? meta.getColumnLabel(i) : meta.getColumnName(i);
      final int columnType = meta.getColumnType(i);
      final String typeName = JDBCType.valueOf(columnType).getName();
      final String columnClassName = meta.getColumnClassName(i);
      final String columnTypeName = meta.getColumnTypeName(i);
      final Schema.Field field =
          new Schema.Field(
              source.name(),
              source.schema(),
              String.format("From sqlType %d %s (%s)", columnType, typeName, columnClassName),
              source.defaultVal());
      field.addProp("columnName", columnName);
      field.addProp("sqlCode", String.valueOf(columnType));
      field.addProp("typeName", typeName);
      field.addProp("columnClassName", columnClassName);
      if (columnTypeName != null) {
        field.addProp("columnTypeName", columnTypeName);
      }
      fields.add(field);
    }
    final Schema record =
        Schema.createRecord(converted.getName(), doc, converted.getNamespace(), false);
    record.addProp("tableName", JdbcAvroSchema.getDatabaseTableName(meta));
    record.addProp("connectionUrl", connectionUrl);
    record.setFields(fields);
    return record;
  }
}
