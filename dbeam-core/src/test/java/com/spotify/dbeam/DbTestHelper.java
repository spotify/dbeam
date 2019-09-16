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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.TimeZone;

public class DbTestHelper {
  static {
    // use UTC timezone for testing
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
  }

  public static Connection createConnection(String url)
      throws ClassNotFoundException, SQLException {
    Class.forName("org.h2.Driver");
    return DriverManager.getConnection(url);
  }

  public static void createFixtures(String url) throws SQLException, ClassNotFoundException {
    try (Connection connection = createConnection(url)) {
      connection.createStatement().execute(Coffee.ddl());
      connection.createStatement().execute(Coffee.COFFEE1.insertStatement());
      connection.createStatement().execute(Coffee.COFFEE2.insertStatement());
      connection.createStatement().executeQuery("SELECT COUNT(*) FROM COFFEES").next();
    }
  }
}
