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

package com.spotify.dbeam.args;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.stream.Collectors;

public class JdbcConnectionUtil {

  private static Map<String, String> driverMapping =
      ImmutableMap.of(
          "postgresql", "org.postgresql.Driver",
          "mysql", "com.mysql.cj.jdbc.Driver",
          "mariadb", "org.mariadb.jdbc.Driver",
          "h2", "org.h2.Driver"
      );

  private static String driverPrefixes =
      driverMapping.keySet().stream()
          .map(k -> "jdbc:" + k)
          .collect(Collectors.joining(", "));

  public static String getDriverClass(final String url) throws ClassNotFoundException {
    final String[] parts = url.split(":", 3);
    Preconditions.checkArgument(
        parts.length > 1 && "jdbc".equals(parts[0]) && driverMapping.containsKey(parts[1]),
        "Invalid jdbc connection URL: %s. Expected one of %s as prefix.",
        url,
        driverPrefixes
    );
    return Class.forName(driverMapping.get(parts[1])).getCanonicalName();
  }
}
