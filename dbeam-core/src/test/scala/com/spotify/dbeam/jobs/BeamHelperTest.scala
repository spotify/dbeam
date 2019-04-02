/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.dbeam.jobs

import java.io.IOException

import com.spotify.dbeam.beam.BeamHelper
import java.time.Duration

import org.apache.beam.sdk.Pipeline.PipelineExecutionException
import org.apache.beam.sdk.PipelineResult
import org.apache.beam.sdk.metrics.MetricResults
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class BeamHelperTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  "BeamHelper" should "throw exception in case pipeline result finish with state FAILED" in {
    val mockResult = new PipelineResult {
      override def waitUntilFinish(): PipelineResult.State = null
      override def getState: PipelineResult.State = null
      override def cancel(): PipelineResult.State = null
      override def waitUntilFinish(duration: org.joda.time.Duration): PipelineResult.State =
        PipelineResult.State.FAILED
      override def metrics(): MetricResults = null
    }

    the[PipelineExecutionException] thrownBy {
      BeamHelper.waitUntilDone(mockResult, Duration.ofMinutes(1))
    } should have message "java.lang.Exception: Job finished with state FAILED"
  }

  "BeamHelper" should "cancel in case of timeout" in {
    val mockResult = new PipelineResult {
      override def waitUntilFinish(): PipelineResult.State = null
      override def getState: PipelineResult.State = null
      override def cancel(): PipelineResult.State = null
      override def waitUntilFinish(duration: org.joda.time.Duration): PipelineResult.State =
        PipelineResult.State.RUNNING
      override def metrics(): MetricResults = null
    }

    the[PipelineExecutionException] thrownBy {
      BeamHelper.waitUntilDone(mockResult, Duration.ofMinutes(1))
    } should have message "java.lang.Exception: Job cancelled after exceeding timeout PT1M"
  }

  "BeamHelper" should "fail after failure to cancel in case of timeout" in {
    val mockResult = new PipelineResult {
      override def waitUntilFinish(): PipelineResult.State = null
      override def getState: PipelineResult.State = null
      override def cancel(): PipelineResult.State =
        throw new IOException("something wrong")
      override def waitUntilFinish(duration: org.joda.time.Duration): PipelineResult.State =
        PipelineResult.State.RUNNING
      override def metrics(): MetricResults = null
    }

    the[PipelineExecutionException] thrownBy {
      BeamHelper.waitUntilDone(mockResult, Duration.ofMinutes(1))
    } should have message
      "java.lang.Exception: Job exceeded timeout of PT1M, but was not possible to cancel, finished with state RUNNING"
  }

}
