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

package com.spotify.dbeam;

import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import org.mockito.Mockito;
import org.postgresql.jdbc.PgArray;

public class TestHelper {

  public static Path createTmpDirPath(final String subNamePrefix) throws IOException {
    final Path path =
        Paths.get(
            System.getProperty("java.io.tmpdir"),
            subNamePrefix + "-" + UUID.randomUUID().toString());
    Files.createDirectories(path);
    path.toFile().deleteOnExit();
    return path;
  }

  public static List<String> listDir(File dir) {
    return Arrays.stream(Objects.requireNonNull(dir.listFiles()))
        .map(File::getName)
        .sorted()
        .collect(Collectors.toList());
  }

  public static UUID byteBufferToUuid(final ByteBuffer byteBuffer) {
    final Long high = byteBuffer.getLong();
    final Long low = byteBuffer.getLong();

    return new UUID(high, low);
  }

  public static void mockResultSetMeta(ResultSetMetaData meta, int columnIdx, int columnType,
                                       String columnName,
                                       String columnClassName, String columnTypeName)
      throws SQLException {
    when(meta.getColumnType(columnIdx)).thenReturn(columnType);
    when(meta.getColumnName(columnIdx)).thenReturn(columnName);
    when(meta.getColumnClassName(columnIdx)).thenReturn(columnClassName);
    when(meta.getColumnTypeName(columnIdx)).thenReturn(columnTypeName);
  }

  public static Array mockDbArray(int baseType, String baseTypeName, Object array)
      throws SQLException {
    Array arr = Mockito.mock(Array.class);
    when(arr.getBaseType()).thenReturn(baseType);
    when(arr.getBaseTypeName()).thenReturn(baseTypeName);
    when(arr.getArray()).thenReturn(array);
    return arr;
  }
}
