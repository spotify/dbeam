/*
 * Copyright 2018 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.dbeam.options

import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util.Optional

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.json.Json
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.testing.http.{MockHttpTransport, MockLowLevelHttpResponse}
import com.google.api.services.cloudkms.v1.model.DecryptResponse
import com.spotify.dbeam.TestHelper
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PasswordReaderTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val dir = TestHelper.createTmpDirName("jdbc-avro-test-").toFile
  private val passwordPath: Path = Paths.get(dir.getAbsolutePath + ".encrypted")

  override def beforeAll(): Unit = {
    Files.write(passwordPath, "something_encrypted".getBytes, StandardOpenOption.CREATE)
  }

  override protected def afterAll(): Unit = {
    Files.delete(passwordPath)
  }

  it should "decrypt password when using --passwordFileKmsEncrypted parameter" in {
    // Using MockHttpTransport, read more at:
    // https://developers.google.com/api-client-library/java/google-http-java-client/unit-testing
    val mockHttpTransport: MockHttpTransport = new MockHttpTransport.Builder()
      .setLowLevelHttpResponse(
        new MockLowLevelHttpResponse().setStatusCode(200)
          .setContentType(Json.MEDIA_TYPE)
          .setContent(
            JacksonFactory.getDefaultInstance().toByteArray(
            new DecryptResponse()
              .encodePlaintext("something_decrypted".getBytes)
            )
        ))
      .build()
    val passwordReader = new PasswordReader(KmsDecrypter.decrypter()
      .project(Optional.of("fake_project"))
      .credentials(Optional.of(new GoogleCredential.Builder().build()))
      .transport(mockHttpTransport)
      .build())

    val options = PipelineOptionsFactory.create().as(classOf[DBeamPipelineOptions])
    options.setPasswordFileKmsEncrypted(passwordPath.toString)

    passwordReader.readPassword(options) should be (Optional.of("something_decrypted"))
  }


}
