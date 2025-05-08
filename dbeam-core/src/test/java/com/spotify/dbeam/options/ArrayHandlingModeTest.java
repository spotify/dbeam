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

import org.junit.Assert;
import org.junit.Test;

public class ArrayHandlingModeTest {
  @Test
  public void validValuesShouldPass() {
    Assert.assertEquals(ArrayHandlingMode.Bytes,
        ArrayHandlingMode.validateValue(ArrayHandlingMode.Bytes));
    Assert.assertEquals(ArrayHandlingMode.TypedMetaFromFirstRow,
        ArrayHandlingMode.validateValue(ArrayHandlingMode.TypedMetaFromFirstRow));
    Assert.assertEquals(ArrayHandlingMode.TypedMetaPostgres,
        ArrayHandlingMode.validateValue(ArrayHandlingMode.TypedMetaPostgres));
  }

  @Test
  public void invalidValueShouldThrow() {
    RuntimeException thrown = Assert.assertThrows(RuntimeException.class,
        () -> ArrayHandlingMode.validateValue("invalid"));
    Assert.assertEquals("Invalid value 'invalid' for array handling mode. Allowed values: "
        + "[bytes, typed_first_row, typed_postgres]",
        thrown.getMessage());
  }
}
