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

public class DbeamQueryBuilderTest {

  @Test
  public void testCtorFromTable() {
    DbeamQueryBuilder wrapper = DbeamQueryBuilder.fromTablename("abc");

    String expected = "SELECT * FROM abc WHERE 1=1";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testCtorRawSqlWithoutWhere() {
    DbeamQueryBuilder wrapper = DbeamQueryBuilder.fromSqlQuery("SELECT * FROM t1");

    String expected = "SELECT * FROM (SELECT * FROM t1) WHERE 1=1";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testCtorCopyEquals() {
    DbeamQueryBuilder q1 = DbeamQueryBuilder.fromSqlQuery("SELECT * FROM t1");
    DbeamQueryBuilder copy = q1.copy();

    Assert.assertEquals(q1, copy);
  }

  @Test
  public void testCtorCopyContentEquals() {
    DbeamQueryBuilder q1 = DbeamQueryBuilder.fromSqlQuery("SELECT * FROM t1");
    DbeamQueryBuilder copy = q1.copy();

    Assert.assertEquals(q1.build(), copy.build());
  }

  @Test
  public void testCtorCopyWithConditionNotEquals() {
    DbeamQueryBuilder q1 = DbeamQueryBuilder.fromSqlQuery("SELECT * FROM t1");
    DbeamQueryBuilder copy = q1.copy();
    copy.withPartitionCondition("pary", "20180101", "20180201");

    Assert.assertNotEquals(q1.build(), copy.build());
  }

  @Test
  public void testCtorCopyWithLimitNotEquals() {
    DbeamQueryBuilder q1 = DbeamQueryBuilder.fromSqlQuery("SELECT * FROM t1");
    DbeamQueryBuilder copy = q1.copy();
    copy.withLimit(3L);

    Assert.assertNotEquals(q1.build(), copy.build());
  }

  @Test
  public void testCtorRawSqlWithWhere() {
    DbeamQueryBuilder wrapper = DbeamQueryBuilder.fromSqlQuery("SELECT * FROM t1 WHERE a > 100");

    String expected = "SELECT * FROM (SELECT * FROM t1 WHERE a > 100) WHERE 1=1";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testRawSqlWithLimit() {
    DbeamQueryBuilder wrapper = DbeamQueryBuilder.fromSqlQuery("SELECT * FROM t1");
    wrapper.withLimit(102L);

    String expected = "SELECT * FROM (SELECT * FROM t1) WHERE 1=1 LIMIT 102";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testRawSqlwithParallelization() {
    DbeamQueryBuilder wrapper = DbeamQueryBuilder.fromSqlQuery("SELECT * FROM t1");
    wrapper.withParallelizationCondition("bucket", 10, 20, true);

    String expected = "SELECT * FROM (SELECT * FROM t1) WHERE 1=1 AND bucket >= 10 AND bucket < 20";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testRawSqlWithPartition() {
    DbeamQueryBuilder wrapper = DbeamQueryBuilder.fromSqlQuery("SELECT * FROM t1");
    wrapper.withPartitionCondition("birthDate", "2018-01-01", "2018-02-01");

    String expected =
        "SELECT * FROM (SELECT * FROM t1) WHERE 1=1"
            + " AND birthDate >= '2018-01-01' AND birthDate < '2018-02-01'";

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
    String expected = "SELECT * FROM (SELECT * FROM coffees WHERE size > 10) WHERE 1=1";

    rawInput.stream().forEach(in -> execAndCompare(in, expected));
  }

  @Test
  public void testItGeneratesQueryForLimits() {
    String input = "SELECT * FROM coffees WHERE size > 10";
    String expected =
        "SELECT MIN(splitCol) as mixy, MAX(splitCol) as maxy "
            + "FROM (SELECT * FROM coffees WHERE size > 10)"
            + " WHERE 1=1 AND partition >= 'a' AND partition < 'd'";

    String actual =
        DbeamQueryBuilder.fromSqlQuery(input)
            .withPartitionCondition("partition", "a", "d")
            .generateQueryToGetLimitsOfSplitColumn("splitCol", "mixy", "maxy")
            .build();
    Assert.assertEquals(expected, actual);
  }

  private void execAndCompare(String rawInput, String expected) {
    String actual = DbeamQueryBuilder.fromSqlQuery(rawInput).build();

    Assert.assertEquals(expected, actual);
  }
}
