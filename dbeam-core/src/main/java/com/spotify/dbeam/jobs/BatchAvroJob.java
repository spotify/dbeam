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
import com.spotify.dbeam.options.OutputOptions;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.beam.sdk.options.PipelineOptions;

public class BatchAvroJob {

  private final JdbcAvroJob job;

  public BatchAvroJob(final JdbcAvroJob job) {
    this.job = job;
  }

  public static void runExport(final String[] cmdLineArgs, String output, String schema,
                               String table) {

    try {
      PipelineOptions pipelineOptions = JdbcAvroJob.buildPipelineOptions(cmdLineArgs);

      pipelineOptions.as(DBeamPipelineOptions.class).setDbSchema(schema);
      pipelineOptions.as(DBeamPipelineOptions.class).setTable(table);
      pipelineOptions.as(OutputOptions.class).setOutput(Paths.get(output,schema,table).toString());

      JdbcAvroJob job = JdbcAvroJob.create(pipelineOptions);

      job.runExport();
    } catch (Exception e) {
      ExceptionHandling.handleException(e);
    }
  }

  public static void main(String[] cmdLineArgs) {

    PipelineOptions options = JdbcAvroJob.buildPipelineOptions(cmdLineArgs);

    try {
      String tableList = options.as(DBeamPipelineOptions.class).getTableList();
      String output = options.as(OutputOptions.class).getOutput();
      int numThreads = options.as(DBeamPipelineOptions.class).getNumThreads();

      ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads);

      try (Stream<String> stream = Files.lines(Paths.get(tableList))) {
        stream
            .map(x -> x.split("\\."))
            .forEach(x -> {
              executor.execute(new AvroJobTask(cmdLineArgs,output,x[0],x[1]));
            });

      }

      executor.shutdown();
    } catch (Exception e) {
      ExceptionHandling.handleException(e);
    }
  }

  static class AvroJobTask implements Runnable {
    final String [] cmdLineArgs;
    final String output;
    final String schema;
    final String table;

    AvroJobTask(String[] cmdLineArgs, String output, String schema, String table) {
      this.cmdLineArgs = cmdLineArgs;
      this.output = output;
      this.schema = schema;
      this.table = table;
    }

    @Override
    public void run() {
      runExport(cmdLineArgs,output,schema,table);
    }
  }
}
