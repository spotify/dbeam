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

import com.google.common.base.Preconditions;

import org.apache.beam.sdk.options.PipelineOptions;

import java.io.IOException;
import java.util.Optional;

public class JdbcExportArgsFactory {

  public static JdbcExportArgs fromPipelineOptions(PipelineOptions options)
      throws ClassNotFoundException, IOException {
    final JdbcExportPipelineOptions exportOptions = options.as(JdbcExportPipelineOptions.class);
    Preconditions.checkArgument(exportOptions.getConnectionUrl() != null,
                                "'connectionUrl' must be defined");

    final JdbcAvroOptions jdbcAvroOptions = JdbcAvroOptions.create(
        JdbcConnectionConfiguration.create(
            JdbcConnectionUtil.getDriverClass(exportOptions.getConnectionUrl()),
            exportOptions.getConnectionUrl())
            .withUsername(exportOptions.getUsername())
            .withPassword(PasswordReader.readPassword(exportOptions).orElse(null)),
        exportOptions.getFetchSize(),
        exportOptions.getAvroCodec());

    return JdbcExportArgs.create(
        jdbcAvroOptions,
        QueryBuilderArgs.create(exportOptions),
        exportOptions.getAvroSchemaNamespace(),
        Optional.ofNullable(exportOptions.getAvroDoc()),
        exportOptions.isUseAvroLogicalTypes()
    );
  }

}
