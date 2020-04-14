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

package com.spotify.dbeam.jobs;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExceptionHandling {

  private static Logger LOGGER = LoggerFactory.getLogger(ExceptionHandling.class);

  private static final Map<Class<? extends Throwable>, Integer> EXIT_CODES =
      ImmutableMap.of(
          NotReadyException.class, 20,
          IOException.class, 41,
          IllegalArgumentException.class, 43,
          SQLException.class, 45,
          Pipeline.PipelineExecutionException.class, 47);

  public static void handleException(final Throwable e) {
    LOGGER.error("Failure: ", e);
    System.exit(exitCode(e));
  }

  static Integer exitCode(final Throwable e) {
    return EXIT_CODES.entrySet().stream()
        .filter(entry -> entry.getKey().isInstance(e))
        .map(Map.Entry::getValue)
        .findFirst()
        .orElse(49);
  }
}
