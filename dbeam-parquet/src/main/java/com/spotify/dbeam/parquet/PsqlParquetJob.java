/*-
 * -\-\-
 * DBeam Parquet
 * --
 * Copyright (C) 2016 - 2025 Spotify AB
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

package com.spotify.dbeam.parquet;

import com.spotify.dbeam.jobs.ExceptionHandling;
import com.spotify.dbeam.jobs.PsqlReplicationCheck;
import java.io.IOException;

public class PsqlParquetJob {

  private final JdbcParquetJob job;
  private final PsqlReplicationCheck psqlReplicationCheck;

  public PsqlParquetJob(final JdbcParquetJob job, final PsqlReplicationCheck psqlReplicationCheck) {
    this.job = job;
    this.psqlReplicationCheck = psqlReplicationCheck;
  }

  public static PsqlParquetJob create(final String[] cmdLineArgs)
      throws IOException, ClassNotFoundException {
    final JdbcParquetJob job = JdbcParquetJob.create(cmdLineArgs);
    PsqlReplicationCheck.validateOptions(job.getJdbcExportArgs());
    final PsqlReplicationCheck psqlReplicationCheck =
        PsqlReplicationCheck.create(job.getJdbcExportArgs());
    return new PsqlParquetJob(job, psqlReplicationCheck);
  }

  public static void main(String[] cmdLineArgs) {
    try {
      final PsqlParquetJob psqlParquetJob = create(cmdLineArgs);
      psqlParquetJob.psqlReplicationCheck.checkReplication();
      psqlParquetJob.job.runExport();
    } catch (Exception e) {
      ExceptionHandling.handleException(e);
    }
  }
}
