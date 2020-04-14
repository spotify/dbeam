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

import java.io.IOException;

public class PsqlAvroJob {

  private final JdbcAvroJob job;
  private final PsqlReplicationCheck psqlReplicationCheck;

  public PsqlAvroJob(final JdbcAvroJob job, final PsqlReplicationCheck psqlReplicationCheck) {
    this.job = job;
    this.psqlReplicationCheck = psqlReplicationCheck;
  }

  public static PsqlAvroJob create(final String[] cmdLineArgs)
      throws IOException, ClassNotFoundException {
    JdbcAvroJob job = JdbcAvroJob.create(cmdLineArgs);
    PsqlReplicationCheck.validateOptions(job.getJdbcExportArgs());
    final PsqlReplicationCheck psqlReplicationCheck =
        PsqlReplicationCheck.create(job.getJdbcExportArgs());
    return new PsqlAvroJob(job, psqlReplicationCheck);
  }

  public static void main(String[] cmdLineArgs) {
    try {
      final PsqlAvroJob psqlAvroJob = create(cmdLineArgs);
      psqlAvroJob.psqlReplicationCheck.checkReplication();
      psqlAvroJob.job.runExport();
    } catch (Exception e) {
      ExceptionHandling.handleException(e);
    }
  }
}
