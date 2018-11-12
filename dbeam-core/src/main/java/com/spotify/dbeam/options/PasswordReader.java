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

package com.spotify.dbeam.options;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.util.Optional;

import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PasswordReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(PasswordReader.class);

  public static Optional<String> readPassword(DBeamPipelineOptions options) throws IOException {
    FileSystems.setDefaultPipelineOptions(options);
    MatchResult.Metadata m;
    if (options.getPasswordFile() != null) {
      m = FileSystems.matchSingleFileSpec(options.getPasswordFile());
      LOGGER.info("Reading password from file: {}", m.resourceId().toString());
      InputStream
          inputStream =
          Channels.newInputStream(FileSystems.open(m.resourceId()));
      return Optional.of(CharStreams.toString(new InputStreamReader(
          inputStream, Charsets.UTF_8)));
    } else {
      return Optional.ofNullable(options.getPassword());
    }
  }

}
