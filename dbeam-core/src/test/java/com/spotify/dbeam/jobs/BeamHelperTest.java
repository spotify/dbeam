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

package com.spotify.dbeam.jobs;

import com.spotify.dbeam.beam.BeamHelper;

import java.io.IOException;
import java.time.Duration;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.junit.Assert;
import org.junit.Test;

public class BeamHelperTest {

  @Test
  public void shouldThrownExceptionInCaseFailedPipelineResult() {
    PipelineResult mockResult = new PipelineResult() {
      @Override
      public State getState() {
        return null;
      }

      @Override
      public State cancel() throws IOException {
        return null;
      }

      @Override
      public State waitUntilFinish(org.joda.time.Duration duration) {
        return PipelineResult.State.FAILED;
      }

      @Override
      public State waitUntilFinish() {
        return null;
      }

      @Override
      public MetricResults metrics() {
        return null;
      }
    };
    try {
      BeamHelper.waitUntilDone(mockResult, Duration.ofMinutes(1));
      Assert.fail("A PipelineExecutionException should be thrown");
    } catch (Pipeline.PipelineExecutionException exception) {
      Assert.assertEquals(
          "java.lang.Exception: Job finished with state FAILED",
          exception.getMessage()
      );
    }
  }

  @Test
  public void shouldCancelInCaseOfTimeout() {
    PipelineResult mockResult = new PipelineResult() {
      @Override
      public State getState() {
        return null;
      }

      @Override
      public State cancel() throws IOException {
        return null;
      }

      @Override
      public State waitUntilFinish(org.joda.time.Duration duration) {
        return State.RUNNING;
      }

      @Override
      public State waitUntilFinish() {
        return null;
      }

      @Override
      public MetricResults metrics() {
        return null;
      }
    };
    try {
      BeamHelper.waitUntilDone(mockResult, Duration.ofMinutes(1));
      Assert.fail("A PipelineExecutionException should be thrown");
    } catch (Pipeline.PipelineExecutionException exception) {
      Assert.assertEquals(
          "java.lang.Exception: Job cancelled after exceeding timeout PT1M",
          exception.getMessage()
      );
    }
  }

  @Test
  public void shouldFailAfterFailureToCancelAfterTimeout() {
    PipelineResult mockResult = new PipelineResult() {
      @Override
      public State getState() {
        return null;
      }

      @Override
      public State cancel() throws IOException {
        throw new IOException("something wrong");
      }

      @Override
      public State waitUntilFinish(org.joda.time.Duration duration) {
        return State.RUNNING;
      }

      @Override
      public State waitUntilFinish() {
        return null;
      }

      @Override
      public MetricResults metrics() {
        return null;
      }
    };
    try {
      BeamHelper.waitUntilDone(mockResult, Duration.ofMinutes(1));
      Assert.fail("A PipelineExecutionException should be thrown");
    } catch (Pipeline.PipelineExecutionException exception) {
      Assert.assertEquals(
          "java.lang.Exception: Job exceeded timeout of PT1M, "
          + "but was not possible to cancel, finished with state RUNNING",
          exception.getMessage()
      );
    }
  }

}
