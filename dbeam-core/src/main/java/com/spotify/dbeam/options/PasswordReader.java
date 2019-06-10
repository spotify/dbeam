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

import java.io.IOException;
import java.util.Optional;
import org.apache.beam.sdk.io.FileSystems;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PasswordReader extends ParameterFileReader {

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
      return Optional.of(readFromFile(options.getPasswordFile()));
    } else {
      return Optional.ofNullable(options.getPassword());
    }
  }
}
