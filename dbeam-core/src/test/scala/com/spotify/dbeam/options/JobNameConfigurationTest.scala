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

import org.apache.beam.sdk.options.{ApplicationNameOptions, PipelineOptionsFactory}
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JobNameConfigurationTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  it should "configure job name" in {
    val pipelineOptions = PipelineOptionsFactory.create()
    pipelineOptions.setJobName(null)

    JobNameConfiguration.configureJobName(pipelineOptions, "some_db", "some_table")

    pipelineOptions.as(classOf[ApplicationNameOptions]).getAppName should be ("JdbcAvroJob")
    pipelineOptions.getJobName should startWith ("dbeam-somedb-sometable-")
  }

}
