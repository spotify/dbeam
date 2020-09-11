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

import com.spotify.dbeam.options.DBeamPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class BatchAvroJob {

  private final JdbcAvroJob job;

  public BatchAvroJob(final JdbcAvroJob job) {
    this.job = job;
  }

  public static BatchAvroJob create(final String[] cmdLineArgs)
          throws IOException, ClassNotFoundException {
    JdbcAvroJob job = JdbcAvroJob.create(cmdLineArgs);

    return new BatchAvroJob(job);
  }

  public static void main(String[] cmdLineArgs) {

    PipelineOptions options = JdbcAvroJob.buildPipelineOptions(cmdLineArgs);

    try {
      String tableList = options.as(DBeamPipelineOptions.class).getTableList();

      try (Stream<String> stream = Files.lines(Paths.get(tableList))) {
        stream.forEach(System.out::println);
      }


    //  final BatchAvroJob batchAvroJob = create(cmdLineArgs);

     // batchAvroJob.job.runExport();
    } catch (Exception e) {
      ExceptionHandling.handleException(e);
    }
  }
}
