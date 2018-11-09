/*
 * Copyright 2018 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.spotify.dbeam.options;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;

import javax.annotation.Nullable;

/**
 * A POJO describing a how to create a JDBC {@link Connection}.
 */
@AutoValue
public abstract class JdbcConnectionConfiguration implements Serializable {
  @Nullable abstract String getDriverClassName();
  @Nullable abstract String getUrl();
  @Nullable abstract String getUsername();
  @Nullable abstract String getPassword();

  abstract Builder builder();

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setDriverClassName(String driverClassName);
    abstract Builder setUrl(String url);
    abstract Builder setUsername(String username);
    abstract Builder setPassword(String password);
    abstract JdbcConnectionConfiguration build();
  }

  public static JdbcConnectionConfiguration create(String driverClassName, String url) {
    Preconditions.checkArgument(driverClassName != null,
                  "DataSourceConfiguration.create(driverClassName, url) called "
                  + "with null driverClassName");
    Preconditions.checkArgument(url != null,
                  "DataSourceConfiguration.create(driverClassName, url) called "
                  + "with null url");
    return new AutoValue_JdbcConnectionConfiguration.Builder()
        .setDriverClassName(driverClassName)
        .setUrl(url)
        .build();
  }

  public JdbcConnectionConfiguration withUsername(String username) {
    return builder().setUsername(username).build();
  }

  public JdbcConnectionConfiguration withPassword(String password) {
    return builder().setPassword(password).build();
  }

  public Connection getConnection() throws Exception {
    Class.forName(getDriverClassName());
    return DriverManager.getConnection(getUrl(), getUsername(), getPassword());
  }
}
