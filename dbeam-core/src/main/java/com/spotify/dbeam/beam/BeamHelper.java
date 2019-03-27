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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.MimeTypes;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BeamHelper {
  private static Logger LOGGER = LoggerFactory.getLogger(BeamHelper.class);

  public static PipelineResult waitUntilDone(PipelineResult result,
                                             Duration exportTimeout) {
    PipelineResult.State state = result.waitUntilFinish(exportTimeout);
    if (!state.equals(PipelineResult.State.DONE)) {
      throw new Pipeline.PipelineExecutionException(
          new Exception("Job finished with state " + state.toString()));
    }
    return result;
  }

  public static void writeToFile(String filename, ByteBuffer contents) throws IOException {
    ResourceId resourceId = FileSystems.matchNewResource(filename, false);
    try (WritableByteChannel out = FileSystems.create(resourceId, MimeTypes.TEXT)) {
      out.write(contents);
    }
  }

  public static void saveStringOnSubPath(String path, String subPath, String contents)
      throws IOException {
    String filename = path.replaceAll("/+$", "") + subPath;
    writeToFile(filename, ByteBuffer.wrap(contents.getBytes(Charset.defaultCharset())));
  }

  private static ObjectMapper MAPPER = new ObjectMapper();

  public static void saveMetrics(Map<String, Long> metrics, String output) {
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

}
