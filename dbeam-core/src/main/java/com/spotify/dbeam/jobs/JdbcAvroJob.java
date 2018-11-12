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

package com.spotify.dbeam.jobs;

import com.google.common.base.Preconditions;

import com.spotify.dbeam.args.JdbcExportArgs;
import com.spotify.dbeam.avro.BeamJdbcAvroSchema;
import com.spotify.dbeam.avro.JdbcAvroIO;
import com.spotify.dbeam.beam.BeamHelper;
import com.spotify.dbeam.beam.MetricsHelper;
import com.spotify.dbeam.options.JdbcExportArgsFactory;
import com.spotify.dbeam.options.OptionsParser;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.avro.Schema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcAvroJob {

  private static Logger LOGGER = LoggerFactory.getLogger(JdbcAvroJob.class);

  private final Pipeline pipeline;
  private final JdbcExportArgs jdbcExportArgs;
  private final String output;
  private final PipelineOptions pipelineOptions;

  public JdbcAvroJob(PipelineOptions pipelineOptions) throws IOException, ClassNotFoundException {
    this.pipelineOptions = pipelineOptions;
    this.pipeline = Pipeline.create(pipelineOptions);
    this.jdbcExportArgs = JdbcExportArgsFactory.fromPipelineOptions(pipelineOptions);
    this.output =  OptionsParser.getOutput(pipelineOptions);
    Preconditions.checkArgument(this.output != null && this.output.length() > 0,
                                "'output' must be defined");
  }

  public void prepareExport() throws Exception {
    final Schema generatedSchema = BeamJdbcAvroSchema.createSchema(
        this.pipeline, jdbcExportArgs);
    BeamHelper.saveStringOnSubPath(output, "/_AVRO_SCHEMA.avsc", generatedSchema.toString(true));
    final List<String> queries = StreamSupport.stream(
        jdbcExportArgs.queryBuilderArgs().buildQueries().spliterator(), false)
        .collect(Collectors.toList());

    for (int i = 0; i < queries.size(); i++) {
      BeamHelper.saveStringOnSubPath(output, String.format("/_queries/query_%d.sql", i),
                                     queries.get(i));
    }
    LOGGER.info("Running queries: {}", queries.toString());

    pipeline.apply("JdbcQueries", Create.of(queries))
        .apply("JdbcAvroSave", JdbcAvroIO.Write.createWrite(
            output,
            ".avro",
            generatedSchema,
            jdbcExportArgs.jdbcAvroOptions()
        ));
  }

  public Pipeline getPipeline() {
    return pipeline;
  }

  public JdbcExportArgs getJdbcExportArgs() {
    return jdbcExportArgs;
  }

  public String getOutput() {
    return output;
  }

  public PipelineOptions getPipelineOptions() {
    return pipelineOptions;
  }

  public PipelineResult runAndWait() {
    return BeamHelper.waitUntilDone(this.pipeline.run());
  }

  public PipelineResult runExport() throws Exception {
    prepareExport();
    final PipelineResult pipelineResult = runAndWait();
    BeamHelper.saveMetrics(MetricsHelper.getMetrics(pipelineResult), output);
    return pipelineResult;
  }

  public static void main(String[] cmdLineArgs) throws Exception {
    final PipelineOptions pipelineOptions = OptionsParser.buildPipelineOptions(cmdLineArgs);
    new JdbcAvroJob(pipelineOptions).runExport();
  }

}
