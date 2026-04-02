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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Objects;
import java.util.TimeZone;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

/**
 * Writes values from a JDBC ResultSet directly to a Parquet RecordConsumer.
 * Each column is written using the appropriate typed method, skipping null fields
 * (Parquet's standard null representation for optional fields).
 */
public class JdbcParquetWriteSupport {

  private static final Calendar CALENDAR = new GregorianCalendar(TimeZone.getTimeZone("UTC"));

  @FunctionalInterface
  interface ColumnWriter {
    void write(RecordConsumer consumer, ResultSet rs) throws SQLException;
  }

  private final MessageType schema;
  private final ColumnWriter[] columnWriters;
  private final int columnCount;

  public JdbcParquetWriteSupport(MessageType schema, ColumnWriter[] columnWriters,
                                 int columnCount) {
    this.schema = schema;
    this.columnWriters = columnWriters;
    this.columnCount = columnCount;
  }

  public static JdbcParquetWriteSupport create(ResultSet resultSet, MessageType schema)
      throws SQLException {
    final ResultSetMetaData meta = resultSet.getMetaData();
    final int columnCount = meta.getColumnCount();
    final ColumnWriter[] writers = new ColumnWriter[columnCount + 1];

    for (int i = 1; i <= columnCount; i++) {
      writers[i] = computeColumnWriter(meta, i);
    }

    return new JdbcParquetWriteSupport(schema, writers, columnCount);
  }

  public MessageType getSchema() {
    return schema;
  }

  /**
   * Write the current row of the ResultSet to the RecordConsumer.
   */
  public void writeRecord(RecordConsumer consumer, ResultSet resultSet) throws SQLException {
    consumer.startMessage();
    for (int i = 1; i <= columnCount; i++) {
      columnWriters[i].write(consumer, resultSet);
    }
    consumer.endMessage();
  }

  static ColumnWriter computeColumnWriter(final ResultSetMetaData meta, final int column)
      throws SQLException {
    final int columnType = meta.getColumnType(column);
    final int fieldIndex = column - 1;
    final String fieldName = meta.getColumnName(column).isEmpty()
        ? meta.getColumnLabel(column) : meta.getColumnName(column);
    final String normalizedName = fieldName.replaceAll("[^A-Za-z0-9_]", "_");

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
      case DATE:
      case TIME:
      case TIME_WITH_TIMEZONE:
        return (consumer, rs) -> {
          final Timestamp timestamp = rs.getTimestamp(column, CALENDAR);
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
        return (consumer, rs) -> {
          final String val = rs.getString(column);
          if (val != null && !rs.wasNull()) {
            consumer.startField(normalizedName, fieldIndex);
            consumer.addBinary(Binary.fromString(val));
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
}
