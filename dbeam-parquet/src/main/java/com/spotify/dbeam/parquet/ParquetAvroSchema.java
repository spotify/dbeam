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

import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * Derives the Avro schema stored in the {@code parquet.avro.schema} footer key (and in {@code
 * _AVRO_SCHEMA.avsc}) from the Parquet {@link MessageType} that is actually used for writing, so
 * the two can never contradict each other.
 *
 * <p>The mapping follows parquet-avro's {@code AvroSchemaConverter} (Parquet to Avro direction)
 * and what {@code AvroParquetReader} can decode with the resulting schema:
 *
 * <ul>
 *   <li>{@code optional} fields become {@code ["null", T]} with a {@code null} default, {@code
 *       required} fields become {@code T}.
 *   <li>3-level {@code LIST} groups become an Avro {@code array} of the element type; an optional
 *       element becomes a nullable item ({@code ["null", T]}).
 *   <li>{@code FIXED_LEN_BYTE_ARRAY(16) (UUID)} becomes {@code string} with logical type {@code
 *       uuid}; parquet-avro decodes the 16 bytes into the canonical UUID string.
 *   <li>{@code TIMESTAMP(MILLIS|MICROS)} become {@code timestamp-millis|micros} ({@code
 *       local-timestamp-*} when not adjusted to UTC), {@code DATE} becomes {@code date}, {@code
 *       TIME} becomes {@code time-millis|micros}, {@code DECIMAL} becomes {@code decimal}.
 * </ul>
 *
 * <p>Implemented in-module rather than depending on parquet-avro at runtime so that the record can
 * keep dbeam's namespace, doc and column properties and the runtime jar does not grow.
 */
public final class ParquetAvroSchema {

  private ParquetAvroSchema() {}

  /** Doc and properties to attach to a top-level Avro field. */
  static final class FieldInfo {
    final String doc;
    final Map<String, String> props;

    FieldInfo(final String doc, final Map<String, String> props) {
      this.doc = doc;
      this.props = props;
    }
  }

  /**
   * Per-column doc and properties, using the same conventions as dbeam-core's {@code
   * JdbcAvroSchema} ({@code doc = "From sqlType ..."}, {@code columnName}, {@code sqlCode}, ...).
   */
  static List<FieldInfo> fieldInfos(final ResultSetMetaData meta) throws SQLException {
    final List<FieldInfo> infos = new ArrayList<>();
    for (int i = 1; i <= meta.getColumnCount(); i++) {
      final String columnName =
          meta.getColumnName(i).isEmpty() ? meta.getColumnLabel(i) : meta.getColumnName(i);
      final int columnType = meta.getColumnType(i);
      final String typeName = JDBCType.valueOf(columnType).getName();
      final String columnClassName = meta.getColumnClassName(i);
      final String columnTypeName = meta.getColumnTypeName(i);
      final Map<String, String> props = new java.util.LinkedHashMap<>();
      props.put("columnName", columnName);
      props.put("sqlCode", String.valueOf(columnType));
      props.put("typeName", typeName);
      if (columnClassName != null) {
        props.put("columnClassName", columnClassName);
      }
      if (columnTypeName != null) {
        props.put("columnTypeName", columnTypeName);
      }
      infos.add(
          new FieldInfo(
              String.format("From sqlType %d %s (%s)", columnType, typeName, columnClassName),
              props));
    }
    return infos;
  }

  public static Schema toAvroSchema(
      final MessageType messageType, final String namespace, final String doc) {
    return toAvroSchema(messageType, namespace, doc, Collections.emptyMap(), null);
  }

  /**
   * Converts {@code messageType} to an Avro record named after the message.
   *
   * @param fieldInfos optional doc/properties per top-level field, by position; ignored unless
   *     its size equals the number of fields
   * @throws IllegalArgumentException for Parquet types without an Avro equivalent (e.g. INT96)
   */
  static Schema toAvroSchema(
      final MessageType messageType,
      final String namespace,
      final String doc,
      final Map<String, String> recordProps,
      final List<FieldInfo> fieldInfos) {
    final List<FieldInfo> infos =
        fieldInfos != null && fieldInfos.size() == messageType.getFieldCount()
            ? fieldInfos
            : null;
    final Set<String> usedNames = new HashSet<>();
    usedNames.add(messageType.getName());
    final List<Schema.Field> fields = new ArrayList<>();
    for (int i = 0; i < messageType.getFieldCount(); i++) {
      final Type type = messageType.getType(i);
      final FieldInfo info = infos == null ? null : infos.get(i);
      final Schema.Field field = toField(type, info == null ? null : info.doc, usedNames);
      if (info != null) {
        info.props.forEach(field::addProp);
      }
      fields.add(field);
    }
    final Schema record = Schema.createRecord(messageType.getName(), doc, namespace, false);
    recordProps.forEach(record::addProp);
    record.setFields(fields);
    return record;
  }

  private static Schema.Field toField(
      final Type type, final String doc, final Set<String> usedNames) {
    final Schema schema = toSchema(type, usedNames);
    if (type.isRepetition(Type.Repetition.OPTIONAL)) {
      return new Schema.Field(type.getName(), optional(schema), doc, JsonProperties.NULL_VALUE);
    }
    return new Schema.Field(type.getName(), schema, doc, (Object) null);
  }

  private static Schema optional(final Schema schema) {
    return Schema.createUnion(Schema.create(Schema.Type.NULL), schema);
  }

  /** Avro schema for {@code type}, ignoring its own repetition except for 2-level lists. */
  private static Schema toSchema(final Type type, final Set<String> usedNames) {
    if (type.isRepetition(Type.Repetition.REPEATED)) {
      // un-annotated repeated field: Parquet 2-level list of required items
      return Schema.createArray(toNonRepeatedSchema(type, usedNames));
    }
    return toNonRepeatedSchema(type, usedNames);
  }

  private static Schema toNonRepeatedSchema(final Type type, final Set<String> usedNames) {
    if (type.isPrimitive()) {
      return toPrimitiveSchema(type.asPrimitiveType(), usedNames);
    }
    final GroupType group = type.asGroupType();
    final LogicalTypeAnnotation annotation = group.getLogicalTypeAnnotation();
    if (annotation instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
      return Schema.createArray(listElementSchema(group, usedNames));
    }
    if (annotation instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation
        || annotation instanceof LogicalTypeAnnotation.MapKeyValueTypeAnnotation) {
      return mapSchema(group, usedNames);
    }
    final List<Schema.Field> fields = new ArrayList<>();
    for (final Type child : group.getFields()) {
      fields.add(toField(child, null, usedNames));
    }
    final Schema record = Schema.createRecord(uniqueName(group.getName(), usedNames), null, null,
        false);
    record.setFields(fields);
    return record;
  }

  private static Schema listElementSchema(final GroupType list, final Set<String> usedNames) {
    if (list.getFieldCount() != 1 || !list.getType(0).isRepetition(Type.Repetition.REPEATED)) {
      throw new IllegalArgumentException("Invalid LIST group: " + list);
    }
    final Type repeated = list.getType(0);
    if (isElementType(repeated, list.getName())) {
      // 2-level list: the repeated field is the (required) element itself
      return toNonRepeatedSchema(repeated, usedNames);
    }
    // 3-level list: repeated group list { <repetition> element; }
    final Type element = repeated.asGroupType().getType(0);
    final Schema elementSchema = toSchema(element, usedNames);
    return element.isRepetition(Type.Repetition.OPTIONAL) ? optional(elementSchema) : elementSchema;
  }

  /** Backward-compatibility rules of parquet-avro AvroSchemaConverter/AvroRecordConverter. */
  private static boolean isElementType(final Type repeated, final String parentName) {
    return repeated.isPrimitive()
        || repeated.asGroupType().getFieldCount() > 1
        || repeated.getName().equals("array")
        || repeated.getName().equals(parentName + "_tuple");
  }

  private static Schema mapSchema(final GroupType map, final Set<String> usedNames) {
    if (map.getFieldCount() != 1 || map.getType(0).isPrimitive()) {
      throw new IllegalArgumentException("Invalid MAP group: " + map);
    }
    final GroupType keyValue = map.getType(0).asGroupType();
    final Type key = keyValue.getType(0);
    if (!key.isPrimitive()
        || key.asPrimitiveType().getPrimitiveTypeName() != PrimitiveType.PrimitiveTypeName.BINARY
        || !(key.getLogicalTypeAnnotation()
            instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation)) {
      throw new IllegalArgumentException("Avro maps require STRING keys: " + map);
    }
    final Type value = keyValue.getType(1);
    final Schema valueSchema = toSchema(value, usedNames);
    return Schema.createMap(
        value.isRepetition(Type.Repetition.OPTIONAL) ? optional(valueSchema) : valueSchema);
  }

  private static Schema toPrimitiveSchema(final PrimitiveType type, final Set<String> usedNames) {
    final LogicalTypeAnnotation annotation = type.getLogicalTypeAnnotation();
    switch (type.getPrimitiveTypeName()) {
      case BOOLEAN:
        return Schema.create(Schema.Type.BOOLEAN);
      case INT32:
        return withLogicalType(Schema.create(Schema.Type.INT), int32LogicalType(annotation));
      case INT64:
        return withLogicalType(Schema.create(Schema.Type.LONG), int64LogicalType(annotation));
      case FLOAT:
        return Schema.create(Schema.Type.FLOAT);
      case DOUBLE:
        return Schema.create(Schema.Type.DOUBLE);
      case BINARY:
        if (annotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation
            || annotation instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation
            || annotation instanceof LogicalTypeAnnotation.JsonLogicalTypeAnnotation) {
          return Schema.create(Schema.Type.STRING);
        }
        return withLogicalType(Schema.create(Schema.Type.BYTES), decimalLogicalType(annotation));
      case FIXED_LEN_BYTE_ARRAY:
        if (annotation instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation) {
          return LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));
        }
        return withLogicalType(
            Schema.createFixed(
                uniqueName(type.getName(), usedNames), null, null, type.getTypeLength()),
            decimalLogicalType(annotation));
      case INT96:
      default:
        throw new IllegalArgumentException(
            "Parquet type " + type + " has no Avro equivalent for the footer schema");
    }
  }

  private static LogicalType int32LogicalType(final LogicalTypeAnnotation annotation) {
    if (annotation instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
      return LogicalTypes.date();
    }
    if (annotation instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation
        && ((LogicalTypeAnnotation.TimeLogicalTypeAnnotation) annotation).getUnit()
            == LogicalTypeAnnotation.TimeUnit.MILLIS) {
      return LogicalTypes.timeMillis();
    }
    return null; // DECIMAL on INT32 stays a plain int, as in AvroSchemaConverter
  }

  private static LogicalType int64LogicalType(final LogicalTypeAnnotation annotation) {
    if (annotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
      final LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts =
          (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) annotation;
      final boolean utc = ts.isAdjustedToUTC();
      switch (ts.getUnit()) {
        case MILLIS:
          return utc ? LogicalTypes.timestampMillis() : LogicalTypes.localTimestampMillis();
        case MICROS:
          return utc ? LogicalTypes.timestampMicros() : LogicalTypes.localTimestampMicros();
        default:
          return null; // NANOS: plain long, as AvroSchemaConverter does
      }
    }
    if (annotation instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation
        && ((LogicalTypeAnnotation.TimeLogicalTypeAnnotation) annotation).getUnit()
            == LogicalTypeAnnotation.TimeUnit.MICROS) {
      return LogicalTypes.timeMicros();
    }
    return null; // DECIMAL on INT64 stays a plain long, as in AvroSchemaConverter
  }

  private static LogicalType decimalLogicalType(final LogicalTypeAnnotation annotation) {
    if (annotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
      final LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal =
          (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) annotation;
      return LogicalTypes.decimal(decimal.getPrecision(), decimal.getScale());
    }
    return null;
  }

  private static Schema withLogicalType(final Schema schema, final LogicalType logicalType) {
    return logicalType == null ? schema : logicalType.addToSchema(schema);
  }

  /** Named Avro types (nested records, fixed) must be unique within the schema. */
  private static String uniqueName(final String name, final Set<String> usedNames) {
    String candidate = name;
    int suffix = 1;
    while (!usedNames.add(candidate)) {
      candidate = name + "_" + suffix++;
    }
    return candidate;
  }
}
