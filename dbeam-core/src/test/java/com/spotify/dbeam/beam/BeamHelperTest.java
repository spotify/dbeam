/*-
 * -\-\-
 * DBeam Core
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

public class BeamHelperTest {

  @Test
  public void shouldWorkWithGcs() throws IOException {
    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create());
    // This test ensures that gcsio / GcsUtil work properly and have the right classes on classpath
    try {
      BeamHelper.readFromFile("gs://does-not-exist-1");
      fail("Expected IOException for non-existent GCS file");
    } catch (IOException e) {
      // Beam changed behavior - now throws IOException instead of FileNotFoundException
      // Verify it's the expected GCS error message
      assertTrue("Exception should indicate GCS file matching error",
          e.getMessage().contains("Error matching file spec") 
          && e.getMessage().contains("gs://does-not-exist-1"));
    }
  }
}
