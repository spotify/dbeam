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
package com.spotify.dbeam.options;

import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.PipelineOptions;

import java.util.concurrent.ThreadLocalRandom;

public class JobNameConfiguration {

  private static String normalizeString(String str) {
    return str.toLowerCase().replaceAll("[^a-z0-9]", "");
  }

  public static void configureJobName(PipelineOptions options, String dbName, String tableName) {
    try {
      options.as(ApplicationNameOptions.class).setAppName("JdbcAvroJob");
    } catch (Exception e) {

    }
    if (options.getJobName() == null) {
      String randomPart = Integer.toHexString(ThreadLocalRandom.current().nextInt());
      options.setJobName(
          String.join("-",
                      "dbeam", normalizeString(dbName), normalizeString(tableName), randomPart));
    }
  }
}
