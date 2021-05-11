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

import com.google.api.client.json.GsonFactory;
import com.google.api.client.json.Json;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.services.cloudkms.v1.model.DecryptResponse;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import org.apache.beam.sdk.extensions.gcp.auth.NoopCredentialFactory;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class PasswordReaderTest {

  private static File passwordFile;

  @BeforeClass
  public static void beforeAll() throws IOException {
    passwordFile = File.createTempFile("pattern", ".suffix");
    passwordFile.deleteOnExit();
    Files.write(passwordFile.toPath(), "something_encrypted".getBytes(), StandardOpenOption.CREATE);
  }

  @AfterClass
  public static void afterAll() throws IOException {
    Files.delete(passwordFile.toPath());
  }

  @Test
  public void shouldDecryptPasswordOnpasswordFileKmsEncryptedParameter() throws IOException {

    // Using MockHttpTransport, read more at:
    // https://developers.google.com/api-client-library/java/google-http-java-client/unit-testing
    MockHttpTransport mockHttpTransport =
        new MockHttpTransport.Builder()
            .setLowLevelHttpResponse(
                new MockLowLevelHttpResponse()
                    .setStatusCode(200)
                    .setContentType(Json.MEDIA_TYPE)
                    .setContent(
                        GsonFactory.getDefaultInstance()
                            .toByteArray(
                                new DecryptResponse()
                                    .encodePlaintext("something_decrypted".getBytes()))))
            .build();
    PasswordReader passwordReader =
        new PasswordReader(
            KmsDecrypter.decrypter()
                .project(Optional.of("fake_project"))
                .credentials(NoopCredentialFactory.fromOptions(null).getCredential())
                .transport(mockHttpTransport)
                .build());

    DBeamPipelineOptions options = PipelineOptionsFactory.create().as(DBeamPipelineOptions.class);
    options.setPasswordFileKmsEncrypted(passwordFile.getPath());

    final Optional<String> actualPassword = passwordReader.readPassword(options);

    Assert.assertEquals(Optional.of("something_decrypted"), actualPassword);
  }
}
