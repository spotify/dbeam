package com.spotify.dbeam;

import com.google.common.collect.ImmutableMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

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

public class JdbcAvroRecord {

  private static final int MAX_DIGITS_BIGINT = 19;
  private static final Calendar CALENDAR = new GregorianCalendar(TimeZone.getTimeZone("UTC"));

  public static GenericRecord convertResultSetIntoAvroRecord(
      Schema schema, ResultSet resultSet, Map<Integer, SQLFunction<ResultSet, Object>> mappings,
      int columnCount)
      throws SQLException {
    final GenericRecord record = new GenericData.Record(schema);
    for (int i=1; i <= columnCount; i++) {
      final Object value = mappings.get(i).apply(resultSet);
      if (!(value == null || resultSet.wasNull())) {
        record.put(i - 1, value);
      }
    }
    return record;
  }

  public static Map<Integer, SQLFunction<ResultSet, Object>> computeAllMappings(ResultSet resultSet)
      throws SQLException {
    final ResultSetMetaData meta = resultSet.getMetaData();
    final int columnCount = meta.getColumnCount();
    final Map<Integer, SQLFunction<ResultSet, Object>> mappings = new HashMap<>(columnCount);

    for (int i = 1; i <= columnCount; i++) {
      mappings.put(i, computeMapping(meta, i));
    }
    return ImmutableMap.copyOf(mappings);
  }

  @FunctionalInterface
  public interface SQLFunction<T, R> {
    R apply(T t) throws SQLException;
  }

  private static ByteBuffer nullableBytes(byte[] bts) {
    if (bts != null) {
      return ByteBuffer.wrap(bts);
    } else {
      return null;
    }
  }

  static SQLFunction<ResultSet, Object> computeMapping(final ResultSetMetaData meta, final int column)
      throws SQLException {
    switch (meta.getColumnType(column)) {
      case VARCHAR: case CHAR: case CLOB:
      case LONGNVARCHAR: case LONGVARCHAR: case NCHAR:
        return resultSet -> resultSet.getString(column);
      case BIGINT:
        final int precision = meta.getPrecision(column);
        if (precision > 0 && precision <= MAX_DIGITS_BIGINT) {
          return resultSet -> resultSet.getLong(column);
        }
        // otherwise return as string
        break;
      case INTEGER: case SMALLINT: case TINYINT:
        return resultSet -> resultSet.getInt(column);
      case TIMESTAMP: case DATE:
      case TIME: case TIME_WITH_TIMEZONE:
        return resultSet -> {
          final Timestamp timestamp = resultSet.getTimestamp(column, CALENDAR);
          if (timestamp != null) {
            return timestamp.getTime();
          } else {
            return null;
          }
        };
      case BOOLEAN:
        return resultSet -> resultSet.getBoolean(column);
      case BIT:
        if (meta.getPrecision(column) <= 1) {
          return resultSet -> resultSet.getBoolean(column);
        } else {
          return resultSet -> nullableBytes(resultSet.getBytes(column));
        }
      case BINARY: case VARBINARY:
      case LONGVARBINARY: case ARRAY:
      case BLOB:
        return resultSet -> nullableBytes(resultSet.getBytes(column));
      case DOUBLE:
        return resultSet -> resultSet.getDouble(column);
      case FLOAT: case REAL:
        return resultSet -> resultSet.getFloat(column);
    }
    return resultSet -> resultSet.getString(column);
  }

}
