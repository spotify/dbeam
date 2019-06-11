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

package com.spotify.dbeam.options;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParameterFileReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParameterFileReader.class);
  private static final Charset DEFAULT_CHARSET_UTF_8 = Charsets.UTF_8;

  public static String readFromFile(String filename) throws IOException {
    MatchResult.Metadata m = FileSystems.matchSingleFileSpec(filename);
    LOGGER.info("Reading data from file: {}", m.resourceId().toString());
    InputStream inputStream = Channels.newInputStream(FileSystems.open(m.resourceId()));
    return CharStreams.toString(new InputStreamReader(inputStream, DEFAULT_CHARSET_UTF_8));
  }

  public String readAsResource(String filename) throws IOException {
    ClassLoader classLoader = getClass().getClassLoader();
    URL resource = classLoader.getResource(filename);
    File file = new File(resource.getFile());
    LOGGER.info("Reading data from file: {}", file.getAbsolutePath());
    return CharStreams.toString(
        new InputStreamReader(new FileInputStream(file), DEFAULT_CHARSET_UTF_8));
  }
}
