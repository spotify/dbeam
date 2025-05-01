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

import static java.sql.Types.*;

import com.spotify.dbeam.options.ArrayHandlingMode;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Objects;
import java.util.TimeZone;

public class JdbcAvroRecord {

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

  static SqlFunction<ResultSet, Object> computeMapping(final ResultSetMetaData meta,
                                                       final int column,
                                                       final String arrayMode)
      throws SQLException {
    switch (meta.getColumnType(column)) {
      case VARCHAR:
      case CHAR:
      case CLOB:
      case LONGNVARCHAR:
      case LONGVARCHAR:
      case NCHAR:
        return resultSet -> resultSet.getString(column);
      case BIGINT:
        // In PostgreSQL, mySQL and H2, BIGINT is 8 bytes and fits into Java long
        return resultSet -> resultSet.getLong(column);
      case INTEGER:
      case SMALLINT:
      case TINYINT:
        if (Long.class.getCanonicalName().equals(meta.getColumnClassName(column))) {
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
      case ARRAY:
        if (arrayMode.equals(ArrayHandlingMode.Bytes)) {
          return resultSet -> nullableBytes(resultSet.getBytes(column));
        } else {
          return resultSet -> resultSet.getArray(column);
        }
      case BINARY:
      case VARBINARY:
      case LONGVARBINARY:
      case BLOB:
        return resultSet -> nullableBytes(resultSet.getBytes(column));
      case DOUBLE:
        return resultSet -> resultSet.getDouble(column);
      case FLOAT:
      case REAL:
        return resultSet -> resultSet.getFloat(column);
      case OTHER:
        if (Objects.equals(meta.getColumnTypeName(column), "uuid")) {
          return resultSet -> resultSet.getObject(column);
        } else {
          return resultSet -> resultSet.getString(column);
        }
      default:
        return resultSet -> resultSet.getString(column);
    }
  }
}
