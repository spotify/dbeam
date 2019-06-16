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
  public void testCtorFromTable() {
    SqlQueryWrapper wrapper = SqlQueryWrapper.ofTablename("abc");

    String expected = "SELECT * FROM abc WHERE 1=1";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testCtorRawSqlWithoutWhere() {
    SqlQueryWrapper wrapper = SqlQueryWrapper.ofRawSql("SELECT * FROM t1");

    String expected = "SELECT * FROM t1 WHERE 1=1";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testCtorRawSqlWithWhere() {
    SqlQueryWrapper wrapper = SqlQueryWrapper.ofRawSql("SELECT * FROM t1 WHERE a > 100");

    String expected = "SELECT * FROM t1 WHERE a > 100";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCtorRawSqlFailedNoSelect() {
    SqlQueryWrapper.ofRawSql("SELE * FROM t1");
    
    Assert.fail("Should not be reached");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCtorRawSqlFailedNoFrom() {
    SqlQueryWrapper.ofRawSql("SELECT * FRAMME t1");

    Assert.fail("Should not be reached");
  }

  @Test
  public void testCtorRawSqlWithLimit() {
    SqlQueryWrapper wrapper = SqlQueryWrapper.ofRawSql("SELECT * FROM t1");
    wrapper.withLimit(102L);

    String expected = "SELECT * FROM t1 WHERE 1=1 LIMIT 102";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testCtorRawSqlwithParallelization() {
    SqlQueryWrapper wrapper = SqlQueryWrapper.ofRawSql("SELECT * FROM t1");
    wrapper.withParallelizationCondition("bucket", 10, 20, true);

    String expected = "SELECT * FROM t1 WHERE 1=1 AND bucket >= 10 AND bucket < 20";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testCtorRawSqlWithPartition() {
    SqlQueryWrapper wrapper = SqlQueryWrapper.ofRawSql("SELECT * FROM t1");
    wrapper.withPartitionCondition("birthDate", "2018-01-01", "2018-02-01");

    String expected =
        "SELECT * FROM t1 WHERE 1=1 AND birthDate >= '2018-01-01' AND birthDate < '2018-02-01'";

    Assert.assertEquals(expected, wrapper.build());
  }

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
        SqlQueryWrapper.ofRawSql(input).withPartitionCondition("partition", "a", "d")
            .generateQueryToGetLimitsOfSplitColumn(
                "splitCol", "mixy", "maxy")
            .build();
    Assert.assertEquals(expected, actual);
  }

  private void execAndCompare(String rawInput, String expected) {
    String actual = SqlQueryWrapper.ofRawSql(rawInput).build();

    Assert.assertEquals(expected, actual);
  }
}
