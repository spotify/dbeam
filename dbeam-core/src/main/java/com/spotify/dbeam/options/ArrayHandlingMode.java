/*-
 * -\-\-
 * DBeam Core
 * --
 * Copyright (C) 2016 - 2025 Spotify AB
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

import java.util.Arrays;
import java.util.List;

public class ArrayHandlingMode {
  public static final String Bytes = "bytes";
  public static final String TypedMetaFromFirstRow = "typed_first_row";
  public static final String TypedMetaPostgres = "typed_postgres";

  public static String validateValue(String value) {
    List<String> possibleValues = Arrays.asList(Bytes, TypedMetaFromFirstRow, TypedMetaPostgres);
    if (value == null || !possibleValues.contains(value)) {
      throw new RuntimeException(
          String.format("Invalid value '%s' for array handling mode. Allowed values: %s",
              value, possibleValues));
    }

    return value;
  }
}
