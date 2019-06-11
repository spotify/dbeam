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

package com.spotify.dbeam.args;

import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class SqlQueryWrapperTest {

  @Test
  public void testItRemovesTrailingSymbols() {
    List<String> rawInput =
        Arrays.asList(
            "SELECT * FROM coffees WHERE size > 10",
            "SELECT * FROM coffees WHERE size > 10\n",
            "SELECT * FROM coffees WHERE size > 10\r",
            "SELECT * FROM coffees WHERE size > 10\n\r",
            "SELECT * FROM coffees WHERE size > 10;",
            "SELECT * FROM coffees WHERE size > 10 ",
            "SELECT * FROM coffees WHERE size > 10; ",
            "SELECT * FROM coffees WHERE size > 10 ;",
            "SELECT * FROM coffees WHERE size > 10;\n");
    String expected = "SELECT * FROM coffees WHERE size > 10";

    rawInput.stream().forEach(in -> execAndCompare(in, expected));
  }

  @Test
  public void testItGeneratesQueryForLimits() {
    String input = "SELECT * FROM coffees WHERE size > 10";
    String expected =
        "SELECT MIN(splitCol) as mixy, MAX(splitCol) as maxy FROM coffees"
                + " WHERE size > 10 AND partition >= 'a' AND partition < 'd'";

    String actual =
        SqlQueryWrapper.ofRawSql(input)
            .generateQueryToGetLimitsOfSplitColumn(
                "splitCol", "mixy", "maxy", "AND partition >= 'a' AND partition < 'd'")
            .getSqlQuery();
    Assert.assertEquals(expected, actual);
  }

  private void execAndCompare(String rawInput, String expected) {
    String actual = SqlQueryWrapper.ofRawSql(rawInput).getSqlQuery();

    Assert.assertEquals(expected, actual);
  }
}
