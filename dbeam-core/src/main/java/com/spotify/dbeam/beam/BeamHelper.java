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

package com.spotify.dbeam.beam;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.MimeTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BeamHelper {
  private static Logger LOGGER = LoggerFactory.getLogger(BeamHelper.class);

  public static PipelineResult waitUntilDone(
      final PipelineResult result, final Duration exportTimeout) {
    // terminal state might be null, such as:
    // {{ @link org.apache.beam.runners.dataflow.DataflowPipelineJob.waitUntilFinish }}
    @Nullable
    final PipelineResult.State terminalState =
        result.waitUntilFinish(org.joda.time.Duration.millis(exportTimeout.toMillis()));
    if (terminalState == null || !terminalState.isTerminal()) {
      try {
        result.cancel();
      } catch (IOException e) {
        throw new Pipeline.PipelineExecutionException(
            new Exception(
                String.format(
                    "Job exceeded timeout of %s, but was not possible to cancel, "
                        + "finished with terminalState %s",
                    exportTimeout.toString(), terminalState),
                e));
      }
      throw new Pipeline.PipelineExecutionException(
          new Exception("Job cancelled after exceeding timeout " + exportTimeout.toString()));
    }
    if (!terminalState.equals(PipelineResult.State.DONE)) {
      throw new Pipeline.PipelineExecutionException(
          new Exception("Job finished with terminalState " + terminalState.toString()));
    }
    return result;
  }

  public static void writeToFile(final String filename, final ByteBuffer contents)
      throws IOException {
    final ResourceId resourceId = FileSystems.matchNewResource(filename, false);
    try (WritableByteChannel out = FileSystems.create(resourceId, MimeTypes.TEXT)) {
      out.write(contents);
    }
  }

  public static void saveStringOnSubPath(
      final String path, final String subPath, final String contents) throws IOException {
    final String filename = path.replaceAll("/+$", "") + subPath;
    writeToFile(filename, ByteBuffer.wrap(contents.getBytes(Charset.defaultCharset())));
  }

  private static ObjectMapper MAPPER = new ObjectMapper();

  public static void saveMetrics(final Map<String, Long> metrics, final String output) {
    try {
      String metricsJson = MAPPER.writeValueAsString(metrics);
      LOGGER.info("Saving metrics: {}", metricsJson);
      saveStringOnSubPath(output, "/_METRICS.json", metricsJson);
      // for backwards compatibility
      saveStringOnSubPath(output, "/_SERVICE_METRICS.json", metricsJson);
    } catch (IOException exception) {
      // only log failures here, avoid to fail export at the end
      LOGGER.error("Failed to save metrics", exception);
    }
  }

  public static String readFromFile(final String fileSpec) throws IOException {
    final MatchResult.Metadata m = FileSystems.matchSingleFileSpec(fileSpec);
    final InputStream inputStream = Channels.newInputStream(FileSystems.open(m.resourceId()));
    return CharStreams.toString(new InputStreamReader(inputStream, Charsets.UTF_8));
  }
}
