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

import static com.google.common.collect.Lists.newArrayList;

import com.google.common.math.Stats;

import com.spotify.dbeam.beam.MetricsHelper;
import com.spotify.dbeam.options.OutputOptions;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BenchJdbcAvroJob {

  public interface BenchJdbcAvroOptions extends PipelineOptions {
    @Description("The JDBC connection url to perform the export.")
    @Default.Integer(3)
    int getExecutions();

    void setExecutions(int value);
  }

  private final PipelineOptions pipelineOptions;
  private List<Map<String, Long>> metrics = newArrayList();

  public BenchJdbcAvroJob(PipelineOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions;
  }

  public static BenchJdbcAvroJob create(String[] cmdLineArgs)
      throws IOException, ClassNotFoundException {
    PipelineOptionsFactory.register(BenchJdbcAvroOptions.class);
    PipelineOptions options = JdbcAvroJob.buildPipelineOptions(cmdLineArgs);
    return new BenchJdbcAvroJob(options);
  }

  public void run() throws Exception {
    int executions = pipelineOptions.as(BenchJdbcAvroOptions.class).getExecutions();
    for (int i = 0; i < executions; i++) {
      String output = String.format("%s/run_%d",
                                    pipelineOptions.as(OutputOptions.class).getOutput(),
                                    i);
      final PipelineResult
          pipelineResult =
          JdbcAvroJob.create(pipelineOptions, output).runExport();
      this.metrics.add(MetricsHelper.getMetrics(pipelineResult));
    }
    System.out.println(tsvMetrics());
  }

  private String tsvMetrics() {
    final List<String>
        columns =
        newArrayList("recordCount", "writeElapsedMs", "msPerMillionRows", "bytesWritten");
    final Collector<CharSequence, ?, String> tabJoining = Collectors.joining("\t");
    final Stream<String> lines = this.metrics.stream().map(
        m ->
            "k\t" +
            Stream.concat(
                columns.stream().map(c ->
                                         m.get(c).toString()),
                Stream.of(String.valueOf(m.get("bytesWritten") / m.get("writeElapsedMs")))
            )
                .collect(tabJoining)
    );
    final List<Stats> stats = Stream.concat(
        columns.stream().map(c ->
                                 Stats.of((Iterable<Long>) this.metrics.stream()
                                     .map(m -> m.get(c))::iterator)
        ), Stream.of(
            Stats
                .of((Iterable<Long>) this.metrics.stream()
                    .map(m -> m.get("bytesWritten") / m.get("writeElapsedMs"))::iterator)
        )).collect(Collectors.toList());
    return Stream.concat(
        Stream.concat(
            Stream.of(Stream.concat(Stream.of("name"), columns.stream()).collect(tabJoining)),
            lines),
        Stream.of(
            "max\t" +
            stats.stream().map(Stats::max).map(String::valueOf).collect(tabJoining),
            "mean\t" +
            stats.stream().map(Stats::mean).map(String::valueOf).collect(tabJoining),
            "min\t" +
            stats.stream().map(Stats::min).map(String::valueOf).collect(tabJoining),
            "stddev\t" +
            stats.stream().map(Stats::populationStandardDeviation).map(String::valueOf).collect(tabJoining)
        )).collect(Collectors.joining("\n"));
  }

  public static void main(String[] cmdLineArgs) {
    try {
      create(cmdLineArgs).run();
    } catch (Exception e) {
      ExceptionHandling.handleException(e);
    }
  }

}
