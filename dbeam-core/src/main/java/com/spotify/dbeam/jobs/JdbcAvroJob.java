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

import com.google.common.base.Preconditions;
import com.spotify.dbeam.args.DbeamQueryBuilder;
import com.spotify.dbeam.args.JdbcExportArgs;
import com.spotify.dbeam.avro.BeamJdbcAvroSchema;
import com.spotify.dbeam.avro.JdbcAvroIO;
import com.spotify.dbeam.beam.BeamHelper;
import com.spotify.dbeam.beam.MetricsHelper;
import com.spotify.dbeam.options.JdbcExportArgsFactory;
import com.spotify.dbeam.options.JdbcExportPipelineOptions;
import com.spotify.dbeam.options.OutputOptions;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcAvroJob {

  private static Logger LOGGER = LoggerFactory.getLogger(JdbcAvroJob.class);

  private final PipelineOptions pipelineOptions;
  private final Pipeline pipeline;
  private final JdbcExportArgs jdbcExportArgs;
  private final String output;

  public JdbcAvroJob(PipelineOptions pipelineOptions, Pipeline pipeline,
                     JdbcExportArgs jdbcExportArgs, String output) {
    this.pipelineOptions = pipelineOptions;
    this.pipeline = pipeline;
    this.jdbcExportArgs = jdbcExportArgs;
    this.output = output;
    Preconditions.checkArgument(this.output != null && this.output.length() > 0,
                                "'output' must be defined");
  }

  public static JdbcAvroJob create(PipelineOptions pipelineOptions)
      throws IOException, ClassNotFoundException {
    // make sure pipeline.run() does not call waitUntilFinish
    // instead we call with an explicit duration/exportTimeout configuration
    pipelineOptions.as(DirectOptions.class).setBlockOnRun(false);
    return new JdbcAvroJob(pipelineOptions,
                           Pipeline.create(pipelineOptions),
                           JdbcExportArgsFactory.fromPipelineOptions(pipelineOptions),
                           pipelineOptions.as(OutputOptions.class).getOutput());
  }

  public static JdbcAvroJob create(String[] cmdLineArgs)
      throws IOException, ClassNotFoundException {
    return create(buildPipelineOptions(cmdLineArgs));
  }

  public static PipelineOptions buildPipelineOptions(String[] cmdLineArgs) {
    PipelineOptionsFactory.register(JdbcExportPipelineOptions.class);
    PipelineOptionsFactory.register(OutputOptions.class);
    return PipelineOptionsFactory.fromArgs(cmdLineArgs).withValidation().create();
  }

  public void prepareExport() throws Exception {
    LOGGER.info("{} {} version {}",
        this.getClass().getPackage().getImplementationTitle(),
        this.getClass().getSimpleName(),
        this.getClass().getPackage().getImplementationVersion());
    final Schema generatedSchema = BeamJdbcAvroSchema.createSchema(
        this.pipeline, jdbcExportArgs);
    BeamHelper.saveStringOnSubPath(output, "/_AVRO_SCHEMA.avsc", generatedSchema.toString(true));
    final List<DbeamQueryBuilder> sqlQueries = StreamSupport.stream(
        jdbcExportArgs
            .queryBuilderArgs()
            .buildQueries(jdbcExportArgs.createConnection())
            .spliterator(),
        false)
        .collect(Collectors.toList());

    List<String> queries = sqlQueries.stream().map(x -> x.toString()).collect(Collectors.toList());
    
    for (int i = 0; i < queries.size(); i++) {
      BeamHelper.saveStringOnSubPath(output, String.format("/_queries/query_%d.sql", i),
                                     queries.get(i).toString());
    }
    LOGGER.info("Running queries: {}", queries.toString());

    pipeline.apply("JdbcQueries", Create.of(queries))
        .apply("JdbcAvroSave", JdbcAvroIO.createWrite(
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
    return BeamHelper.waitUntilDone(this.pipeline.run(),
                                    jdbcExportArgs.exportTimeout());
  }

  public PipelineResult runExport() throws Exception {
    prepareExport();
    final PipelineResult pipelineResult = runAndWait();
    BeamHelper.saveMetrics(MetricsHelper.getMetrics(pipelineResult), output);
    return pipelineResult;
  }

  public static void main(String[] cmdLineArgs) {
    try {
      JdbcAvroJob.create(cmdLineArgs).runExport();
    } catch (Exception e) {
      ExceptionHandling.handleException(e);
    }
  }

}
