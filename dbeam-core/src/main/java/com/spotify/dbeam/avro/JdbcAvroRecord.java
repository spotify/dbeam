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

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class JdbcAvroRecord {

  static final int MAX_DIGITS_BIGINT = 19;
  private static final Calendar CALENDAR = new GregorianCalendar(TimeZone.getTimeZone("UTC"));

  @FunctionalInterface
  public interface SqlFunction<T, R> {

    R apply(T t) throws SQLException;
  }

  private static ByteBuffer nullableBytes(final byte[] bts) {
    if (bts != null) {
      return ByteBuffer.wrap(bts);
    } else {
      return null;
    }
  }

  static SqlFunction<ResultSet, Object> computeMapping(
      final ResultSetMetaData meta, final int column) throws SQLException {
    
    final String LongClassName = Long.class.getCanonicalName();
    final String columnClassName = meta.getColumnClassName(column);
    switch (meta.getColumnType(column)) {
      case VARCHAR:
      case CHAR:
      case CLOB:
      case LONGNVARCHAR:
      case LONGVARCHAR:
      case NCHAR:
        return resultSet -> resultSet.getString(column);
      case BIGINT:
        final int precision = meta.getPrecision(column);
        if (precision > 0 && precision <= MAX_DIGITS_BIGINT) {
          return resultSet -> resultSet.getLong(column);
        }
        // otherwise return as string
        return resultSet -> resultSet.getString(column);
      case INTEGER:
      case SMALLINT:
      case TINYINT:
        if (LongClassName.equals(columnClassName)) {
          return resultSet -> resultSet.getLong(column);
        }
        return resultSet -> resultSet.getInt(column);
      case TIMESTAMP:
      case DATE:
      case TIME:
      case TIME_WITH_TIMEZONE:
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
      case BINARY:
      case VARBINARY:
      case LONGVARBINARY:
      case ARRAY:
      case BLOB:
        return resultSet -> nullableBytes(resultSet.getBytes(column));
      case DOUBLE:
        return resultSet -> resultSet.getDouble(column);
      case FLOAT:
      case REAL:
        return resultSet -> resultSet.getFloat(column);
      default:
        return resultSet -> resultSet.getString(column);
    }
  }
}
