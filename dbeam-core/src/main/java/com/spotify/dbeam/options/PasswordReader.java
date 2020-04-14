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

class PasswordReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(PasswordReader.class);
  private final KmsDecrypter kmsDecrypter;

  PasswordReader(KmsDecrypter kmsDecrypter) {
    this.kmsDecrypter = kmsDecrypter;
  }

  static final PasswordReader INSTANCE = new PasswordReader(KmsDecrypter.decrypter().build());

  Optional<String> readPassword(DBeamPipelineOptions options) throws IOException {
    FileSystems.setDefaultPipelineOptions(options);
    if (options.getPasswordFileKmsEncrypted() != null) {
      LOGGER.info("Decrypting password using KMS...");
      return Optional.of(kmsDecrypter.decrypt(readFromFile(options.getPasswordFileKmsEncrypted()))
          .trim());
    } else if (options.getPasswordFile() != null) {
      LOGGER.info("Reading password from file: {}", options.getPasswordFile());
      return Optional.of(readFromFile(options.getPasswordFile()));
    } else {
      return Optional.ofNullable(options.getPassword());
    }
  }

  static String readFromFile(String passwordFile) throws IOException {
    MatchResult.Metadata m = FileSystems.matchSingleFileSpec(passwordFile);
    InputStream inputStream = Channels.newInputStream(FileSystems.open(m.resourceId()));
    return CharStreams.toString(new InputStreamReader(inputStream, Charsets.UTF_8));
  }

}
