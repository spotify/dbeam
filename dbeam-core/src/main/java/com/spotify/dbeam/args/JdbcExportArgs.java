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

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.sql.Connection;
import java.time.Duration;
import java.util.Optional;
import org.apache.avro.Schema;

@AutoValue
public abstract class JdbcExportArgs implements Serializable {

  private static final long serialVersionUID = 10595393104L;

  public abstract JdbcAvroArgs jdbcAvroOptions();

  public abstract QueryBuilderArgs queryBuilderArgs();

  public abstract String avroSchemaNamespace();

  public abstract Optional<String> avroSchemaName();

  public abstract Optional<String> avroDoc();

  public abstract Boolean useAvroLogicalTypes();

  public abstract Duration exportTimeout();

  public abstract Optional<Schema> inputAvroSchema();

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setJdbcAvroOptions(JdbcAvroArgs jdbcAvroArgs);

    abstract Builder setQueryBuilderArgs(QueryBuilderArgs queryBuilderArgs);

    abstract Builder setAvroSchemaNamespace(String avroSchemaNamespace);

    abstract Builder setAvroSchemaName(Optional<String> avroSchemaName);

    abstract Builder setAvroDoc(Optional<String> avroDoc);

    abstract Builder setUseAvroLogicalTypes(Boolean useAvroLogicalTypes);

    abstract Builder setExportTimeout(Duration exportTimeout);

    abstract Builder setInputAvroSchema(Optional<Schema> inputAvroSchema);

    abstract JdbcExportArgs build();
  }

  @VisibleForTesting
  static JdbcExportArgs create(
      final JdbcAvroArgs jdbcAvroArgs, final QueryBuilderArgs queryBuilderArgs) {
    return create(
        jdbcAvroArgs,
        queryBuilderArgs,
        "dbeam_generated",
        Optional.empty(),
        Optional.empty(),
        false,
        Duration.ofDays(7),
        Optional.empty());
  }

  public static JdbcExportArgs create(
      final JdbcAvroArgs jdbcAvroArgs,
      final QueryBuilderArgs queryBuilderArgs,
      final String avroSchemaNamespace,
      final Optional<String> avroSchemaName,
      final Optional<String> avroDoc,
      final Boolean useAvroLogicalTypes,
      final Duration exportTimeout,
      final Optional<Schema> inputAvroSchema) {
    return new AutoValue_JdbcExportArgs.Builder()
        .setJdbcAvroOptions(jdbcAvroArgs)
        .setQueryBuilderArgs(queryBuilderArgs)
        .setAvroSchemaNamespace(avroSchemaNamespace)
        .setAvroSchemaName(avroSchemaName)
        .setAvroDoc(avroDoc)
        .setUseAvroLogicalTypes(useAvroLogicalTypes)
        .setExportTimeout(exportTimeout)
        .setInputAvroSchema(inputAvroSchema)
        .build();
  }

  public Connection createConnection() throws Exception {
    return this.jdbcAvroOptions().jdbcConnectionConfiguration().createConnection();
  }
}
