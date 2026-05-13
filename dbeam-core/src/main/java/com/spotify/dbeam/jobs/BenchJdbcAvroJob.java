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

import org.apache.beam.sdk.options.PipelineOptionsFactory;

/** Used on e2e test, allows benchmarking with different configuration parameters. */
public class BenchJdbcAvroJob {

  public static BenchJdbcJob create(final String[] cmdLineArgs) {
    PipelineOptionsFactory.register(BenchJdbcJob.BenchJdbcOptions.class);
    return BenchJdbcJob.create(
        "BenchJdbcAvroJob",
        cmdLineArgs,
        JdbcAvroJob::buildPipelineOptions,
        (opts, output) -> JdbcAvroJob.create(opts, output).runExport());
  }

  public static void main(String[] cmdLineArgs) {
    try {
      create(cmdLineArgs).run();
    } catch (Exception e) {
      ExceptionHandling.handleException(e);
    }
  }
}
