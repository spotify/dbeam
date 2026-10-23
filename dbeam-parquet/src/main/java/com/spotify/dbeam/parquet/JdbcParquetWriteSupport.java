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
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TIME_WITH_TIMEZONE;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Objects;
import java.util.TimeZone;
import java.util.UUID;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * Writes values from a JDBC ResultSet directly to a Parquet RecordConsumer. Each column is written
 * using the appropriate typed method, skipping null fields (Parquet's standard null representation
 * for optional fields).
 */
public class JdbcParquetWriteSupport {

  private static final ThreadLocal<Calendar> CALENDAR =
      ThreadLocal.withInitial(() -> new GregorianCalendar(TimeZone.getTimeZone("UTC")));

  private static final long MILLIS_PER_DAY = 86_400_000L;

  @FunctionalInterface
  interface ColumnWriter {
    void write(RecordConsumer consumer, ResultSet rs) throws SQLException;
  }

  private final MessageType schema;
  private final ColumnWriter[] columnWriters;
  private final int columnCount;

  public JdbcParquetWriteSupport(
      MessageType schema, ColumnWriter[] columnWriters, int columnCount) {
    this.schema = schema;
    this.columnWriters = columnWriters;
    this.columnCount = columnCount;
  }

  public static JdbcParquetWriteSupport create(
      ResultSet resultSet, MessageType schema, String arrayMode, boolean useTimestampMicros)
      throws SQLException {
    return create(resultSet, schema, arrayMode, useTimestampMicros, false);
  }

  public static JdbcParquetWriteSupport create(
      ResultSet resultSet,
      MessageType schema,
      String arrayMode,
      boolean useTimestampMicros,
      boolean nativeTypes)
      throws SQLException {
    final ResultSetMetaData meta = resultSet.getMetaData();
    final int columnCount = meta.getColumnCount();
    final ColumnWriter[] writers = new ColumnWriter[columnCount + 1];

    for (int i = 1; i <= columnCount; i++) {
      writers[i] =
          computeColumnWriter(
              meta, i, arrayMode, schema.getType(i - 1), useTimestampMicros, nativeTypes);
    }

    return new JdbcParquetWriteSupport(schema, writers, columnCount);
  }

  public MessageType getSchema() {
    return schema;
  }

  /** Write the current row of the ResultSet to the RecordConsumer. */
  public void writeRecord(RecordConsumer consumer, ResultSet resultSet) throws SQLException {
    consumer.startMessage();
    for (int i = 1; i <= columnCount; i++) {
      columnWriters[i].write(consumer, resultSet);
    }
    consumer.endMessage();
  }

  /**
   * Timestamp/date/time columns are written as epoch micros when {@code useTimestampMicros} is set
   * and as epoch millis otherwise, regardless of the target field's annotation (a schema file whose
   * annotation disagrees is rejected by {@link ParquetSchemaValidator}).
   */
  static ColumnWriter computeColumnWriter(
      final ResultSetMetaData meta,
      final int column,
      final String arrayMode,
      final Type fieldType,
      final boolean useTimestampMicros)
      throws SQLException {
    return computeColumnWriter(meta, column, arrayMode, fieldType, useTimestampMicros, false);
  }

  /**
   * With {@code nativeTypes} ({@code --parquetNativeTypes}), DATE, TIME and DECIMAL/NUMERIC
   * columns are written as described in {@link JdbcParquetSchema#buildNativeFieldType}; like the
   * schema, this depends only on the JDBC metadata and the flags.
   */
  static ColumnWriter computeColumnWriter(
      final ResultSetMetaData meta,
      final int column,
      final String arrayMode,
      final Type fieldType,
      final boolean useTimestampMicros,
      final boolean nativeTypes)
      throws SQLException {
    final int columnType = meta.getColumnType(column);
    final int fieldIndex = column - 1;
    final String fieldName =
        meta.getColumnName(column).isEmpty()
            ? meta.getColumnLabel(column)
            : meta.getColumnName(column);
    final String normalizedName = JdbcParquetSchema.normalizeFieldName(fieldName);

    if (nativeTypes) {
      final ColumnWriter nativeWriter =
          nativeColumnWriter(meta, column, fieldIndex, normalizedName, useTimestampMicros);
      if (nativeWriter != null) {
        return nativeWriter;
      }
    }

    switch (columnType) {
      case VARCHAR:
      case CHAR:
      case CLOB:
      case LONGNVARCHAR:
      case LONGVARCHAR:
      case NCHAR:
        return (consumer, rs) -> {
          final String val = rs.getString(column);
          if (val != null && !rs.wasNull()) {
            consumer.startField(normalizedName, fieldIndex);
            consumer.addBinary(Binary.fromString(val));
            consumer.endField(normalizedName, fieldIndex);
          }
        };
      case BIGINT:
        return (consumer, rs) -> {
          final long val = rs.getLong(column);
          if (!rs.wasNull()) {
            consumer.startField(normalizedName, fieldIndex);
            consumer.addLong(val);
            consumer.endField(normalizedName, fieldIndex);
          }
        };
      case INTEGER:
      case SMALLINT:
      case TINYINT:
        if (Long.class.getCanonicalName().equals(meta.getColumnClassName(column))) {
          return (consumer, rs) -> {
            final long val = rs.getLong(column);
            if (!rs.wasNull()) {
              consumer.startField(normalizedName, fieldIndex);
              consumer.addLong(val);
              consumer.endField(normalizedName, fieldIndex);
            }
          };
        }
        return (consumer, rs) -> {
          final int val = rs.getInt(column);
          if (!rs.wasNull()) {
            consumer.startField(normalizedName, fieldIndex);
            consumer.addInteger(val);
            consumer.endField(normalizedName, fieldIndex);
          }
        };
      case TIMESTAMP:
      case DATE: // written as epoch millis/micros for Avro-path compatibility
      case TIME:
      case TIME_WITH_TIMEZONE:
        if (useTimestampMicros) {
          return (consumer, rs) -> {
            final Timestamp timestamp = rs.getTimestamp(column, CALENDAR.get());
            if (timestamp != null && !rs.wasNull()) {
              consumer.startField(normalizedName, fieldIndex);
              consumer.addLong(toEpochMicros(timestamp));
              consumer.endField(normalizedName, fieldIndex);
            }
          };
        }
        return (consumer, rs) -> {
          final Timestamp timestamp = rs.getTimestamp(column, CALENDAR.get());
          if (timestamp != null && !rs.wasNull()) {
            consumer.startField(normalizedName, fieldIndex);
            consumer.addLong(timestamp.getTime());
            consumer.endField(normalizedName, fieldIndex);
          }
        };
      case BOOLEAN:
        return (consumer, rs) -> {
          final boolean val = rs.getBoolean(column);
          if (!rs.wasNull()) {
            consumer.startField(normalizedName, fieldIndex);
            consumer.addBoolean(val);
            consumer.endField(normalizedName, fieldIndex);
          }
        };
      case BIT:
        if (meta.getPrecision(column) <= 1) {
          return (consumer, rs) -> {
            final boolean val = rs.getBoolean(column);
            if (!rs.wasNull()) {
              consumer.startField(normalizedName, fieldIndex);
              consumer.addBoolean(val);
              consumer.endField(normalizedName, fieldIndex);
            }
          };
        } else {
          return (consumer, rs) -> {
            final byte[] val = rs.getBytes(column);
            if (val != null && !rs.wasNull()) {
              consumer.startField(normalizedName, fieldIndex);
              consumer.addBinary(Binary.fromConstantByteArray(val));
              consumer.endField(normalizedName, fieldIndex);
            }
          };
        }
      case ARRAY:
        if ("bytes".equals(arrayMode)) {
          return (consumer, rs) -> {
            final byte[] val = rs.getBytes(column);
            if (val != null && !rs.wasNull()) {
              consumer.startField(normalizedName, fieldIndex);
              consumer.addBinary(Binary.fromConstantByteArray(val));
              consumer.endField(normalizedName, fieldIndex);
            }
          };
        }
        final String arrayElementType =
            writtenArrayElementType(
                JdbcParquetSchema.resolveArrayElementTypeName(meta.getColumnTypeName(column)),
                fieldType);
        final boolean nullableItems = hasOptionalListElement(fieldType);
        return (consumer, rs) -> {
          final java.sql.Array sqlArray = rs.getArray(column);
          if (sqlArray != null && !rs.wasNull()) {
            final Object[] items = (Object[]) sqlArray.getArray();
            // check before writing anything, so no partial or garbage list is written
            for (Object item : items) {
              if (isNestedArray(item, arrayElementType)) {
                throw multidimensionalArrayException(fieldName);
              }
              if (item == null && !nullableItems) {
                throw nullArrayItemException(fieldName);
              }
            }
            consumer.startField(normalizedName, fieldIndex);
            consumer.startGroup(); // LIST group
            consumer.startField("list", 0); // repeated list field
            for (Object item : items) {
              consumer.startGroup(); // list element group
              if (item != null) {
                consumer.startField("element", 0);
                writeArrayElement(consumer, item, arrayElementType);
                consumer.endField("element", 0);
              }
              consumer.endGroup();
            }
            consumer.endField("list", 0);
            consumer.endGroup();
            consumer.endField(normalizedName, fieldIndex);
          }
        };
      case BINARY:
      case VARBINARY:
      case LONGVARBINARY:
      case BLOB:
        return (consumer, rs) -> {
          final byte[] val = rs.getBytes(column);
          if (val != null && !rs.wasNull()) {
            consumer.startField(normalizedName, fieldIndex);
            consumer.addBinary(Binary.fromConstantByteArray(val));
            consumer.endField(normalizedName, fieldIndex);
          }
        };
      case DOUBLE:
        return (consumer, rs) -> {
          final double val = rs.getDouble(column);
          if (!rs.wasNull()) {
            consumer.startField(normalizedName, fieldIndex);
            consumer.addDouble(val);
            consumer.endField(normalizedName, fieldIndex);
          }
        };
      case FLOAT:
      case REAL:
        return (consumer, rs) -> {
          final float val = rs.getFloat(column);
          if (!rs.wasNull()) {
            consumer.startField(normalizedName, fieldIndex);
            consumer.addFloat(val);
            consumer.endField(normalizedName, fieldIndex);
          }
        };
      case OTHER:
        if (Objects.equals(meta.getColumnTypeName(column), "uuid")) {
          final boolean isUuidLogicalType =
              fieldType.getLogicalTypeAnnotation() != null
                  && fieldType.getLogicalTypeAnnotation().equals(LogicalTypeAnnotation.uuidType());
          if (isUuidLogicalType) {
            return (consumer, rs) -> {
              final Object val = rs.getObject(column);
              if (val != null && !rs.wasNull()) {
                final UUID uuid =
                    val instanceof UUID ? (UUID) val : UUID.fromString(val.toString());
                final ByteBuffer buf = ByteBuffer.allocate(16);
                buf.putLong(uuid.getMostSignificantBits());
                buf.putLong(uuid.getLeastSignificantBits());
                consumer.startField(normalizedName, fieldIndex);
                consumer.addBinary(Binary.fromConstantByteArray(buf.array()));
                consumer.endField(normalizedName, fieldIndex);
              }
            };
          }
          return (consumer, rs) -> {
            final Object val = rs.getObject(column);
            if (val != null && !rs.wasNull()) {
              consumer.startField(normalizedName, fieldIndex);
              consumer.addBinary(Binary.fromString(val.toString()));
              consumer.endField(normalizedName, fieldIndex);
            }
          };
        }
        // fall through to string
      default:
        return (consumer, rs) -> {
          final String val = rs.getString(column);
          if (val != null && !rs.wasNull()) {
            consumer.startField(normalizedName, fieldIndex);
            consumer.addBinary(Binary.fromString(val));
            consumer.endField(normalizedName, fieldIndex);
          }
        };
    }
  }

  private static ColumnWriter nativeColumnWriter(
      final ResultSetMetaData meta,
      final int column,
      final int fieldIndex,
      final String name,
      final boolean useTimestampMicros)
      throws SQLException {
    switch (meta.getColumnType(column)) {
      case DATE:
        return (consumer, rs) -> {
          final Timestamp timestamp = rs.getTimestamp(column, CALENDAR.get());
          if (timestamp != null && !rs.wasNull()) {
            consumer.startField(name, fieldIndex);
            consumer.addInteger(
                Math.toIntExact(Math.floorDiv(timestamp.getTime(), MILLIS_PER_DAY)));
            consumer.endField(name, fieldIndex);
          }
        };
      case TIME:
      case TIME_WITH_TIMEZONE:
        if (useTimestampMicros) {
          return (consumer, rs) -> {
            final Timestamp timestamp = rs.getTimestamp(column, CALENDAR.get());
            if (timestamp != null && !rs.wasNull()) {
              consumer.startField(name, fieldIndex);
              consumer.addLong(Math.floorMod(toEpochMicros(timestamp), MILLIS_PER_DAY * 1000L));
              consumer.endField(name, fieldIndex);
            }
          };
        }
        return (consumer, rs) -> {
          final Timestamp timestamp = rs.getTimestamp(column, CALENDAR.get());
          if (timestamp != null && !rs.wasNull()) {
            consumer.startField(name, fieldIndex);
            consumer.addInteger((int) Math.floorMod(timestamp.getTime(), MILLIS_PER_DAY));
            consumer.endField(name, fieldIndex);
          }
        };
      case DECIMAL:
      case NUMERIC:
        final int precision = meta.getPrecision(column);
        final int scale = meta.getScale(column);
        if (!JdbcParquetSchema.isNativeDecimal(precision, scale)) {
          return null;
        }
        final int byteLength = JdbcParquetSchema.decimalByteLength(precision);
        return (consumer, rs) -> {
          final BigDecimal value = rs.getBigDecimal(column);
          if (value != null && !rs.wasNull()) {
            final BigInteger unscaled = unscaledDecimal(value, precision, scale, name);
            consumer.startField(name, fieldIndex);
            consumer.addBinary(Binary.fromConstantByteArray(toFixed(unscaled, byteLength)));
            consumer.endField(name, fieldIndex);
          }
        };
      default:
        return null;
    }
  }

  /**
   * Unscaled value of {@code value} at {@code scale}. Fails instead of rounding or truncating if
   * the value has more fractional digits than {@code scale} or more digits than {@code
   * precision}.
   */
  static BigInteger unscaledDecimal(
      final BigDecimal value, final int precision, final int scale, final String column) {
    final BigDecimal rescaled;
    try {
      rescaled = value.setScale(scale, RoundingMode.UNNECESSARY);
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException(
          String.format(
              "Value %s in column '%s' has more than %d fractional digits and cannot be written"
                  + " as DECIMAL(%d,%d) without rounding",
              value.toPlainString(), column, scale, precision, scale),
          e);
    }
    if (rescaled.precision() > precision && rescaled.signum() != 0) {
      throw new IllegalArgumentException(
          String.format(
              "Value %s in column '%s' exceeds the precision of DECIMAL(%d,%d)",
              value.toPlainString(), column, precision, scale));
    }
    return rescaled.unscaledValue();
  }

  /** Big-endian two's complement, sign-extended to {@code length} bytes. */
  static byte[] toFixed(final BigInteger unscaled, final int length) {
    final byte[] bytes = unscaled.toByteArray();
    final byte[] fixed = new byte[length];
    final byte pad = (byte) (unscaled.signum() < 0 ? 0xFF : 0x00);
    final int offset = length - bytes.length;
    for (int i = 0; i < offset; i++) {
      fixed[i] = pad;
    }
    System.arraycopy(bytes, 0, fixed, offset, bytes.length);
    return fixed;
  }

  /** Epoch microseconds, keeping the sub-millisecond part of {@link Timestamp#getNanos()}. */
  static long toEpochMicros(final Timestamp timestamp) {
    return timestamp.getTime() * 1000 + (timestamp.getNanos() / 1000) % 1000;
  }

  /**
   * Whether an ARRAY element is itself an array (a multidimensional SQL array). A {@code byte[]}
   * is the value of a {@code bytea} element, so only {@code byte[][]} and deeper count there.
   */
  static boolean isNestedArray(final Object item, final String elementType) {
    if (item == null) {
      return false;
    }
    if (item instanceof java.sql.Array) {
      return true;
    }
    if (item instanceof byte[]) {
      return !"bytea".equals(elementType);
    }
    return item.getClass().isArray();
  }

  /**
   * Whether the LIST element of {@code fieldType} is {@code optional} ({@code
   * --nullableArrayItems}). Elements of the derived schema are {@code required} without the flag,
   * and a schema file must agree (see {@link ParquetSchemaValidator}).
   */
  /**
   * An element type without a Parquet mapping is written as a string (toString()) when the schema
   * declares the element as {@code binary (STRING)}, which only {@code
   * --parquetUnsupportedArrayItemsAsString} produces; otherwise it is kept, and rejected on write.
   */
  static String writtenArrayElementType(final String elementType, final Type fieldType) {
    if (isKnownArrayElementType(elementType) || fieldType == null || fieldType.isPrimitive()) {
      return elementType;
    }
    final Type repeated = fieldType.asGroupType().getType(0);
    if (repeated.isPrimitive() || repeated.asGroupType().getFieldCount() == 0) {
      return elementType;
    }
    final Type element = repeated.asGroupType().getType(0);
    final boolean isString =
        element.isPrimitive()
            && element.asPrimitiveType().getPrimitiveTypeName()
                == PrimitiveType.PrimitiveTypeName.BINARY
            && LogicalTypeAnnotation.stringType().equals(element.getLogicalTypeAnnotation());
    return isString ? "text" : elementType;
  }

  private static boolean isKnownArrayElementType(final String elementType) {
    switch (elementType) {
      case "int":
      case "int4":
      case "int2":
      case "int8":
      case "float4":
      case "float8":
      case "bool":
      case "bytea":
        return true;
      default:
        return JdbcParquetSchema.isStringArrayElementType(elementType);
    }
  }

  static boolean hasOptionalListElement(final Type fieldType) {
    if (fieldType == null || fieldType.isPrimitive()) {
      return true;
    }
    final Type repeated = fieldType.asGroupType().getType(0);
    if (repeated.isPrimitive() || repeated.asGroupType().getFieldCount() == 0) {
      return true;
    }
    return repeated.asGroupType().getType(0).getRepetition() != Type.Repetition.REQUIRED;
  }

  /** Same message as the Avro export (dbeam-core JdbcAvroRecordConverter). */
  static RuntimeException nullArrayItemException(final String columnName) {
    return new RuntimeException(
        String.format("Array item is null in column '%s', use --nullableArrayItems", columnName));
  }

  static IllegalArgumentException multidimensionalArrayException(final String columnName) {
    return new IllegalArgumentException(
        String.format(
            "Column '%s' contains a multidimensional array, which is not supported;"
                + " use --arrayMode=bytes to export the column as bytes",
            columnName));
  }

  static void writeArrayElement(RecordConsumer consumer, Object item, String elementType) {
    switch (elementType) {
      case "int":
      case "int4":
      case "int2":
        consumer.addInteger(((Number) item).intValue());
        break;
      case "int8":
        consumer.addLong(((Number) item).longValue());
        break;
      case "float4":
        consumer.addFloat(((Number) item).floatValue());
        break;
      case "float8":
        consumer.addDouble(((Number) item).doubleValue());
        break;
      case "bool":
        consumer.addBoolean((Boolean) item);
        break;
      case "bytea":
        consumer.addBinary(Binary.fromConstantByteArray((byte[]) item));
        break;
      default:
        if (!JdbcParquetSchema.isStringArrayElementType(elementType)) {
          // rejected by JdbcParquetSchema.buildArrayElementType, unless the schema came elsewhere
          throw new IllegalArgumentException(
              "ARRAY element type '" + elementType + "' is not supported for typed Parquet arrays;"
                  + " use --arrayMode=bytes to export the column as bytes");
        }
        consumer.addBinary(Binary.fromString(item.toString()));
        break;
    }
  }
}
