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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.cloudkms.v1.CloudKMS;
import com.google.api.services.cloudkms.v1.CloudKMSScopes;
import com.google.api.services.cloudkms.v1.model.DecryptRequest;
import com.google.api.services.cloudkms.v1.model.DecryptResponse;
import com.google.auto.value.AutoValue;
import com.google.cloud.ServiceOptions;
import com.google.common.base.CharMatcher;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KmsSecrets {

  private static final Logger LOGGER = LoggerFactory.getLogger(PasswordReader.class);
  private static final String KEYRING;
  private static final String KEY;
  private static final String LOCATION;

  static {
    Properties p = System.getProperties();
    KEYRING = p.getProperty("KMS_KEYRING", "dbeam");
    KEY = p.getProperty("KMS_KEY", "default");
    LOCATION = p.getProperty("KMS_LOCATION", "global");
  }

  /**
   * Create a new {@link Decrypter.Builder} for customizing the {@link Decrypter} configuration.
   */
  public static Decrypter.Builder decrypter() {
    return new AutoValue_KmsSecrets_Decrypter.Builder()
        .location(LOCATION)
        .key(KEY)
        .keyring(KEYRING)
        .project(ServiceOptions.getDefaultProjectId());
  }

  @AutoValue
  public abstract static class Decrypter {

    /**
     * The location of the KMS key to use for decryption.
     */
    abstract String location();

    /**
     * The ring of the KMS key to use for decryption.
     */
    abstract String keyring();

    /**
     * The name of the KMS key to use for decryption.
     */
    abstract String key();

    /**
     * The GCP project KMS key to use for decryption. Will be detected from credentials
     * or gcloud sdk if not set.
     */
    abstract String project();

    /**
     * The {@link HttpTransport} to use for the default credentials and KMS client.
     * Default will be used if not set.
     */
    abstract Optional<HttpTransport> transport();

    /**
     * The {@link JsonFactory} to use for the default credentials and KMS client.
     * Default will be used if not set.
     */
    abstract Optional<JsonFactory> jsonFactory();

    /**
     * The {@link GoogleCredential} to use for the KMS client. Default will be used if not set.
     */
    abstract Optional<GoogleCredential> credentials();

    public abstract Builder builder();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder location(String location);

      public abstract Builder keyring(String keyring);

      public abstract Builder key(String keyring);

      public abstract Builder project(String project);

      public abstract Builder transport(HttpTransport transport);

      public abstract Builder jsonFactory(Optional<JsonFactory> jsonFactory);

      public abstract Builder credentials(Optional<GoogleCredential> credentials);

      public abstract Decrypter build();
    }

    /**
     * Decrypt a base64 encoded cipher text string.
     */
    String decrypt(String base64Ciphertext) throws IOException {
      return StandardCharsets.UTF_8.decode(decryptBinary(base64Ciphertext)).toString();
    }

    /**
     * Decrypt a base64 encoded cipher text string.
     *
     * @return A {@link ByteBuffer} with the raw contents.
     */
    ByteBuffer decryptBinary(String base64Ciphertext) throws IOException {
      final String keyName = String.format(
          "projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s",
          project(), location(), keyring(), key());

      final DecryptResponse response = kms()
          .projects().locations().keyRings().cryptoKeys()
          .decrypt(keyName, new DecryptRequest()
              .setCiphertext(CharMatcher.WHITESPACE.removeFrom(base64Ciphertext)))
          .execute();
      return ByteBuffer.wrap(Base64.getDecoder().decode(response.getPlaintext()));
    }

    private CloudKMS kms() throws IOException {
      final HttpTransport transport = transport().orElseGet(Utils::getDefaultTransport);
      final JsonFactory jsonFactory = jsonFactory().orElseGet(Utils::getDefaultJsonFactory);
      final GoogleCredential googleCredential =
          credentials().isPresent() ? credentials().get()
                                    : GoogleCredential.getApplicationDefault();
      return KmsSecrets.kms(transport, jsonFactory, googleCredential);
    }

  }

  private static CloudKMS kms(HttpTransport transport,
                              JsonFactory jsonFactory, GoogleCredential credential) {
    return new CloudKMS.Builder(transport, jsonFactory, scoped(credential))
        .setApplicationName("dbeam")
        .build();
  }

  private static GoogleCredential scoped(GoogleCredential credential) {
    if (credential.createScopedRequired()) {
      return credential.createScoped(CloudKMSScopes.all());
    }
    return credential;
  }

}
