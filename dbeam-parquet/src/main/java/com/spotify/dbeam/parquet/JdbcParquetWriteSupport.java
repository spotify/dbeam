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

import com.spotify.dbeam.options.ArrayHandlingMode;
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
import java.util.Locale;
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
      final ResultSet resultSet, final MessageType schema, final ParquetSchemaOptions options)
      throws SQLException {
    final ResultSetMetaData meta = resultSet.getMetaData();
    final int columnCount = meta.getColumnCount();
    final ColumnWriter[] writers = new ColumnWriter[columnCount + 1];

    for (int i = 1; i <= columnCount; i++) {
      writers[i] = computeColumnWriter(meta, i, schema.getType(i - 1), options);
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
   * Writer for one column, matching {@link JdbcParquetSchema#buildParquetFieldType} for the same
   * options. Timestamp/date/time columns are written as epoch micros with {@code
   * useTimestampMicros} and as epoch millis otherwise, regardless of the target field's annotation
   * (a schema file whose annotation disagrees is rejected by {@link ParquetSchemaValidator}). With
   * {@code nativeTypes} ({@code --parquetNativeTypes}), DATE, TIME and DECIMAL/NUMERIC columns are
   * written as described in {@link JdbcParquetSchema#buildNativeFieldType}. Values are written to
   * the field of {@code fieldType}, whose name may differ from the column's in a schema file.
   */
  static ColumnWriter computeColumnWriter(
      final ResultSetMetaData meta,
      final int column,
      final Type fieldType,
      final ParquetSchemaOptions options)
      throws SQLException {
    final String arrayMode = options.arrayMode();
    final boolean useTimestampMicros = options.useTimestampMicros();
    final int columnType = meta.getColumnType(column);
    final int fieldIndex = column - 1;
    final String fieldName =
        meta.getColumnName(column).isEmpty()
            ? meta.getColumnLabel(column)
            : meta.getColumnName(column);
    final String normalizedName = fieldType.getName();

    if (options.nativeTypes()) {
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
                fieldType,
                JdbcParquetSchema.resolveArrayElementTypeName(meta.getColumnTypeName(column)),
                arrayMode,
                fieldName);
        final boolean nullableItems = hasOptionalListElement(fieldType);
        return (consumer, rs) -> {
          final java.sql.Array sqlArray = rs.getArray(column);
          if (sqlArray != null && !rs.wasNull()) {
            final Object[] items = (Object[]) sqlArray.getArray();
            // check before writing anything, so no partial or garbage list is written
            checkArrayItems(items, arrayElementType, nullableItems, fieldName);
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
   * precision}. The error messages leave out the value so that row data does not end up in task
   * exceptions.
   */
  static BigInteger unscaledDecimal(
      final BigDecimal value, final int precision, final int scale, final String column) {
    final BigDecimal rescaled;
    try {
      rescaled = value.setScale(scale, RoundingMode.UNNECESSARY);
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException(
          String.format(
              "A value in column '%s' has more than %d fractional digits and cannot be written"
                  + " as DECIMAL(%d,%d) without rounding",
              column, scale, precision, scale),
          e);
    }
    if (rescaled.precision() > precision && rescaled.signum() != 0) {
      throw new IllegalArgumentException(
          String.format(
              "A value in column '%s' exceeds the precision of DECIMAL(%d,%d)",
              column, precision, scale));
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
      return !"bytea".equals(elementType) && !"binary".equals(elementType);
    }
    return item.getClass().isArray();
  }

  /**
   * The element type to write for a LIST field, reconciling the schema's element with {@code
   * metadataElementType} (from the column type name, see {@link
   * JdbcParquetSchema#resolveArrayElementTypeName}):
   *
   * <ul>
   *   <li>a supported metadata type must match the schema element ({@code int4}/{@code int2} with
   *       INT32, {@code int8} with INT64, {@code float4} with FLOAT, {@code float8} with DOUBLE,
   *       {@code bool} with BOOLEAN, {@code bytea} with an unannotated binary, string-like types
   *       with a binary) and is written as itself;
   *   <li>an unsupported metadata type (e.g. {@code timestamp}, {@code numeric}, or a generic
   *       {@code ARRAY} name) is written with the value's toString() into a binary element: {@code
   *       STRING} as derived with {@code --parquetUnsupportedArrayItemsAsString}, or the
   *       unannotated, {@code JSON} or {@code ENUM} binary a schema file may use (an unannotated
   *       one takes {@code byte[]} values as bytes). With {@code typed_first_row}, where the
   *       schema element came from the first row's values, it also follows a non-binary element.
   * </ul>
   *
   * <p>Anything else fails, instead of converting values to a type the column does not have
   * (schema files are rejected earlier by {@link ParquetSchemaValidator}; this guards the writer).
   */
  static String writtenArrayElementType(
      final Type fieldType,
      final String metadataElementType,
      final String arrayMode,
      final String columnName) {
    final String schemaElementType = schemaArrayElementType(fieldType);
    if (schemaElementType == null) {
      return metadataElementType;
    }
    final String expected;
    if (JdbcParquetSchema.isStringArrayElementType(metadataElementType)) {
      expected = "text";
    } else if ("int".equals(metadataElementType) || "int2".equals(metadataElementType)) {
      expected = "int4";
    } else if ("bytea".equals(metadataElementType)) {
      expected = "binary";
    } else if (JdbcParquetSchema.isSupportedArrayElementType(metadataElementType)) {
      expected = metadataElementType;
    } else if ("text".equals(schemaElementType)
        || "binary".equals(schemaElementType)
        || ArrayHandlingMode.TypedMetaFromFirstRow.equals(arrayMode)) {
      return schemaElementType;
    } else {
      throw arrayElementMismatch(columnName, metadataElementType, fieldType);
    }
    final boolean matches =
        expected.equals(schemaElementType)
            // a string column may use an unannotated binary element (a schema file's choice)
            || ("text".equals(expected) && "binary".equals(schemaElementType));
    if (!matches) {
      throw arrayElementMismatch(columnName, metadataElementType, fieldType);
    }
    return "binary".equals(expected) ? "bytea" : metadataElementType;
  }

  /**
   * {@code int4}, {@code int8}, {@code float4}, {@code float8}, {@code bool}, {@code text} (an
   * annotated binary) or {@code binary} (an unannotated one) for the LIST element of {@code
   * fieldType}; {@code fixed}-style and other elements give the primitive's name; null when {@code
   * fieldType} is not a LIST.
   */
  private static String schemaArrayElementType(final Type fieldType) {
    if (fieldType == null || fieldType.isPrimitive()) {
      return null;
    }
    final Type repeated = fieldType.asGroupType().getType(0);
    if (repeated.isPrimitive() || repeated.asGroupType().getFieldCount() == 0) {
      return null;
    }
    final Type element = repeated.asGroupType().getType(0);
    if (!element.isPrimitive()) {
      return null;
    }
    switch (element.asPrimitiveType().getPrimitiveTypeName()) {
      case INT32:
        return "int4";
      case INT64:
        return "int8";
      case FLOAT:
        return "float4";
      case DOUBLE:
        return "float8";
      case BOOLEAN:
        return "bool";
      case BINARY:
        return element.getLogicalTypeAnnotation() == null ? "binary" : "text";
      default:
        return element.asPrimitiveType().getPrimitiveTypeName().name().toLowerCase(Locale.ROOT);
    }
  }

  private static IllegalArgumentException arrayElementMismatch(
      final String columnName, final String metadataElementType, final Type fieldType) {
    return new IllegalArgumentException(
        String.format(
            "Column '%s' has ARRAY element type '%s', which cannot be written to the schema's"
                + " LIST element %s",
            columnName,
            metadataElementType,
            fieldType.asGroupType().getType(0).asGroupType().getType(0)));
  }

  /**
   * Fail on a multidimensional array or, without {@code nullableItems}, a NULL item. All elements
   * of a SQL array share its dimensions, so only the first non-null item is checked for nesting;
   * NULL items are only scanned for when they are not allowed.
   */
  static void checkArrayItems(
      final Object[] items,
      final String elementType,
      final boolean nullableItems,
      final String columnName) {
    boolean checkedNesting = false;
    for (Object item : items) {
      if (item == null) {
        if (!nullableItems) {
          throw nullArrayItemException(columnName);
        }
      } else if (!checkedNesting) {
        if (isNestedArray(item, elementType)) {
          throw multidimensionalArrayException(columnName);
        }
        checkedNesting = true;
        if (nullableItems) {
          return;
        }
      }
    }
  }

  /**
   * Whether the LIST element of {@code fieldType} is {@code optional} ({@code
   * --nullableArrayItems}). Elements of the derived schema are {@code required} without the flag,
   * and a schema file must agree (see {@link ParquetSchemaValidator}).
   */
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
      case "binary":
        consumer.addBinary(
            item instanceof byte[]
                ? Binary.fromConstantByteArray((byte[]) item)
                : Binary.fromString(item.toString()));
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
