package com.spotify.dbeam;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collections;
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
      Schema schema, ResultSet resultSet, Map<Integer, SQLFunction<ResultSet, Object>> mappings)
      throws SQLException {
    GenericRecord record = new GenericData.Record(schema);
    ResultSetMetaData meta = resultSet.getMetaData();

    for (int i=1; i <= meta.getColumnCount(); i++) {
      Object value = mappings.get(i).apply(resultSet);
      if (!resultSet.wasNull() && value != null) {
        record.put(i - 1, value);
      }
    }
    return record;
  }

  public static Map<Integer, SQLFunction<ResultSet, Object>> computeAllMappings(ResultSet resultSet)
      throws SQLException {
    ResultSetMetaData meta = resultSet.getMetaData();
    Map<Integer, SQLFunction<ResultSet, Object>> mappings = new HashMap<>();

    for (int i = 1; i <= meta.getColumnCount(); i++) {
      mappings.put(i, computeMapping(meta, i));
    }
    return Collections.unmodifiableMap(mappings);
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

  static SQLFunction<ResultSet, Object> computeMapping(ResultSetMetaData meta, int column)
      throws SQLException {
    final int columnType = meta.getColumnType(column);
    System.out.println(String.format("%d col -> %d %d", column, columnType, meta.getPrecision(column)));
    if (columnType == VARCHAR || columnType == CHAR ||
        columnType == CLOB || columnType == LONGNVARCHAR ||
        columnType == LONGVARCHAR || columnType == NCHAR) {
      return resultSet -> resultSet.getString(column);
    } else if (columnType == BIGINT) {
      final int precision = meta.getPrecision(column);
      if (precision > 0 && precision <= MAX_DIGITS_BIGINT) {
        return resultSet -> resultSet.getLong(column);
      }
      // otherwise return as string
    } else if (columnType == INTEGER || columnType == SMALLINT || columnType == TINYINT) {
      return resultSet -> resultSet.getInt(column);
    } else if (columnType == TIMESTAMP || columnType == DATE || columnType == TIME || columnType == TIME_WITH_TIMEZONE) {
      return resultSet -> {
        final Timestamp timestamp = resultSet.getTimestamp(column, CALENDAR);
        if (timestamp != null) {
          return timestamp.getTime();
        } else {
          return null;
        }
      };
    } else if (columnType == BOOLEAN || (columnType == BIT && meta.getPrecision(column) <= 1)) {
      return resultSet -> resultSet.getBoolean(column);
    } else if (columnType == BINARY || columnType == VARBINARY ||
               columnType == LONGVARBINARY || columnType == BIT ||
               columnType == ARRAY ||
               columnType == BLOB) {
      return resultSet -> nullableBytes(resultSet.getBytes(column));
    } else if (columnType == DOUBLE) {
      return resultSet -> resultSet.getDouble(column);
    } else if (column == FLOAT || columnType == REAL) {
      return resultSet -> resultSet.getFloat(column);
    }
    return resultSet -> resultSet.getString(column);
  }

}
