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

import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.cloudkms.v1.CloudKMS;
import com.google.api.services.cloudkms.v1.CloudKMSScopes;
import com.google.api.services.cloudkms.v1.model.DecryptRequest;
import com.google.api.services.cloudkms.v1.model.DecryptResponse;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auto.value.AutoValue;
import com.google.cloud.ServiceOptions;
import com.google.common.base.CharMatcher;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;
import java.util.Properties;

@AutoValue
public abstract class KmsDecrypter {

  private static final String KEYRING;
  private static final String KEY;
  private static final String LOCATION;
  private static final String PROJECT;

  static {
    Properties p = System.getProperties();
    KEYRING = p.getProperty("KMS_KEYRING", "dbeam");
    KEY = p.getProperty("KMS_KEY", "default");
    LOCATION = p.getProperty("KMS_LOCATION", "global");
    PROJECT = p.getProperty("KMS_PROJECT");
  }

  /**
   * Create a new {@link Builder} for customizing the {@link KmsDecrypter} configuration.
   *
   * @return a configured {@link KmsDecrypter}
   */
  public static Builder decrypter() {
    return new AutoValue_KmsDecrypter.Builder()
        .location(LOCATION)
        .key(KEY)
        .keyring(KEYRING)
        .project(Optional.ofNullable(PROJECT));
  }

  /** The location of the KMS key to use for decryption. */
  abstract String location();

  /** The ring of the KMS key to use for decryption. */
  abstract String keyring();

  /** The name of the KMS key to use for decryption. */
  abstract String key();

  /**
   * The GCP project KMS key to use for decryption. Will be detected from credentials or gcloud sdk
   * if not set.
   */
  abstract Optional<String> project();

  /**
   * The {@link HttpTransport} to use for the default credentials and KMS client. Default will be
   * used if not set.
   */
  abstract Optional<HttpTransport> transport();

  /** The {@link Credentials} to use for the KMS client. Default will be used if not set. */
  abstract Optional<Credentials> credentials();

  public abstract Builder builder();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder location(String location);

    public abstract Builder keyring(String keyring);

    public abstract Builder key(String keyring);

    public abstract Builder project(Optional<String> project);

    public abstract Builder transport(HttpTransport transport);

    public abstract Builder credentials(Credentials credentials);

    public abstract KmsDecrypter build();
  }

  /** Decrypt a base64 encoded cipher text string. */
  String decrypt(final String base64Ciphertext) throws IOException {
    return StandardCharsets.UTF_8.decode(decryptBinary(base64Ciphertext)).toString();
  }

  /**
   * Decrypt a base64 encoded cipher text string.
   *
   * @return A {@link ByteBuffer} with the raw contents.
   */
  ByteBuffer decryptBinary(final String base64Ciphertext) throws IOException {
    final String project = project().orElseGet(ServiceOptions::getDefaultProjectId);
    final String keyName =
        String.format(
            "projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s",
            project, location(), keyring(), key());

    final DecryptResponse response =
        kms()
            .projects()
            .locations()
            .keyRings()
            .cryptoKeys()
            .decrypt(
                keyName,
                new DecryptRequest()
                    .setCiphertext(CharMatcher.whitespace().removeFrom(base64Ciphertext)))
            .execute();
    return ByteBuffer.wrap(Base64.getDecoder().decode(response.getPlaintext()));
  }

  private CloudKMS kms() throws IOException {
    final HttpTransport transport = transport().orElseGet(Utils::getDefaultTransport);
    final JsonFactory jsonFactory = Utils.getDefaultJsonFactory();
    final Credentials credentials =
        credentials().isPresent() ? credentials().get() : GoogleCredentials.getApplicationDefault();
    return KmsDecrypter.kms(
        transport, jsonFactory, new HttpCredentialsAdapter(scoped(credentials)));
  }

  private static CloudKMS kms(
      final HttpTransport transport,
      final JsonFactory jsonFactory,
      final HttpRequestInitializer httpRequestInitializer) {
    return new CloudKMS.Builder(transport, jsonFactory, httpRequestInitializer)
        .setApplicationName("dbeam")
        .build();
  }

  private static Credentials scoped(final Credentials credentials) {
    if (credentials instanceof GoogleCredentials) {
      return ((GoogleCredentials) credentials).createScoped(CloudKMSScopes.all());
    }
    return credentials;
  }
}
