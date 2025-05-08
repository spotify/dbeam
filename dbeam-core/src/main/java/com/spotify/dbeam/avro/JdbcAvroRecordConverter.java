/*-
 * -\-\-
 * DBeam Core
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.UUID;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

public class JdbcAvroRecordConverter {

  private final JdbcAvroRecord.SqlFunction<ResultSet, Object>[] mappings;
  private final int columnCount;
  private final ResultSet resultSet;
  private final EncoderFactory encoderFactory = EncoderFactory.get();
  private final boolean nullableArrayItems;

  public JdbcAvroRecordConverter(
      final JdbcAvroRecord.SqlFunction<ResultSet, Object>[] mappings,
      final int columnCount,
      final ResultSet resultSet,
      final boolean nullableArrayItems) {
    this.mappings = mappings;
    this.columnCount = columnCount;
    this.resultSet = resultSet;
    this.nullableArrayItems = nullableArrayItems;
  }

  public static JdbcAvroRecordConverter create(final ResultSet resultSet,
                                               final String arrayMode,
                                               final boolean nullableArrayItems)
      throws SQLException {
    return new JdbcAvroRecordConverter(
        computeAllMappings(resultSet, arrayMode),
        resultSet.getMetaData().getColumnCount(),
        resultSet,
        nullableArrayItems);
  }

  @SuppressWarnings("unchecked")
  static JdbcAvroRecord.SqlFunction<ResultSet, Object>[] computeAllMappings(
      final ResultSet resultSet, final String arrayMode)
      throws SQLException {
    final ResultSetMetaData meta = resultSet.getMetaData();
    final int columnCount = meta.getColumnCount();

    final JdbcAvroRecord.SqlFunction<ResultSet, Object>[] mappings =
        (JdbcAvroRecord.SqlFunction<ResultSet, Object>[])
            new JdbcAvroRecord.SqlFunction<?, ?>[columnCount + 1];

    for (int i = 1; i <= columnCount; i++) {
      mappings[i] = JdbcAvroRecord.computeMapping(meta, i, arrayMode);
    }
    return mappings;
  }

  private BinaryEncoder binaryEncoder = null;

  public static class MyByteArrayOutputStream extends ByteArrayOutputStream {

    MyByteArrayOutputStream(int size) {
      super(size);
    }

    // provide access to internal buffer, avoiding copy
    byte[] getBufffer() {
      return buf;
    }
  }

  /**
   * Read data from a single row of result set and and encode into a Avro record as byte array.
   * Directly reading and encoding has the benefit of less need for copying bytes between objects.
   *
   * @return a ByteBuffer with binary encoded Avro record
   * @throws SQLException in case reading row from JDBC fails
   * @throws IOException in case binary encoding fails
   */
  public ByteBuffer convertResultSetIntoAvroBytes() throws SQLException, IOException {
    final MyByteArrayOutputStream out = new MyByteArrayOutputStream(columnCount * 64);
    binaryEncoder = encoderFactory.directBinaryEncoder(out, binaryEncoder);
    for (int i = 1; i <= columnCount; i++) {
      final Object value = mappings[i].apply(resultSet);
      if (value == null || resultSet.wasNull()) {
        binaryEncoder.writeIndex(0);
        binaryEncoder.writeNull();
      } else {
        binaryEncoder.writeIndex(1);
        writeValue(value, resultSet.getMetaData().getColumnName(i), binaryEncoder);
      }
    }
    binaryEncoder.flush();
    return ByteBuffer.wrap(out.getBufffer(), 0, out.size());
  }

  private void writeValue(Object value, String column, BinaryEncoder binaryEncoder)
      throws SQLException, IOException {
    if (value instanceof String) {
      binaryEncoder.writeString((String) value);
    } else if (value instanceof UUID) {
      binaryEncoder.writeString(value.toString());
    } else if (value instanceof Long) {
      binaryEncoder.writeLong((Long) value);
    } else if (value instanceof Integer) {
      binaryEncoder.writeInt((Integer) value);
    } else if (value instanceof Boolean) {
      binaryEncoder.writeBoolean((Boolean) value);
    } else if (value instanceof ByteBuffer) {
      binaryEncoder.writeBytes((ByteBuffer) value);
    } else if (value instanceof Double) {
      binaryEncoder.writeDouble((Double) value);
    } else if (value instanceof Float) {
      binaryEncoder.writeFloat((Float) value);
    } else if (value instanceof java.sql.Array) {
      binaryEncoder.writeArrayStart();
      Object[] array = (Object[]) ((java.sql.Array) value).getArray();
      binaryEncoder.setItemCount(array.length);
      for (Object arrayItem : array) {
        binaryEncoder.startItem();
        if (nullableArrayItems) {
          if (arrayItem == null) {
            binaryEncoder.writeIndex(1);
            binaryEncoder.writeNull();
          } else {
            binaryEncoder.writeIndex(0);
            writeValue(arrayItem, column, binaryEncoder);
          }
        } else {
          if (arrayItem == null) {
            throw new RuntimeException(
                String.format("Array item is null in column '%s', use --nullableArrayItems",
                    column));
          }

          writeValue(arrayItem, column, binaryEncoder);
        }
      }

      binaryEncoder.writeArrayEnd();
    } else {
      throw new RuntimeException(
          String.format("Value of type %s in column '%s' is not supported", value.getClass(),
              column));
    }
  }
}
