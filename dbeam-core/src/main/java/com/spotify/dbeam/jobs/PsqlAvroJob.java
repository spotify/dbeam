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

package com.spotify.dbeam.jobs;

import com.spotify.dbeam.options.OptionsParser;

import org.apache.beam.sdk.options.PipelineOptions;

public class PsqlAvroJob {

  public static void main(String[] cmdLineArgs) throws Exception {
    final PipelineOptions pipelineOptions = OptionsParser.buildPipelineOptions(cmdLineArgs);
    JdbcAvroJob job = new JdbcAvroJob(pipelineOptions);
    PsqlReplicationCheck.validateOptions(job.getJdbcExportArgs());

    if (PsqlReplicationCheck.create(job.getJdbcExportArgs()).isReplicationDelayed()) {
      System.exit(20);
    } else {
      job.runExport();
    }
  }

}
