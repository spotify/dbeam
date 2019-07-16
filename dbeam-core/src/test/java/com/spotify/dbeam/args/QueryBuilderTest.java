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

public class QueryBuilderTest {

  @Test
  public void testCtorFromTable() {
    QueryBuilder wrapper = QueryBuilder.fromTablename("abc");

    String expected = "SELECT * FROM abc WHERE 1=1";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testCtorRawSqlWithoutWhere() {
    QueryBuilder wrapper = QueryBuilder.fromSqlQuery("SELECT * FROM t1");

    String expected = "SELECT * FROM (SELECT * FROM t1) WHERE 1=1";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testCtorCopyEquals() {
    QueryBuilder q1 = QueryBuilder.fromSqlQuery("SELECT * FROM t1");
    QueryBuilder copy = q1.copy();

    Assert.assertEquals(q1, copy);
  }

  @Test
  public void testCtorCopyContentEquals() {
    QueryBuilder q1 = QueryBuilder.fromSqlQuery("SELECT * FROM t1");
    QueryBuilder copy = q1.copy();

    Assert.assertEquals(q1.build(), copy.build());
  }

  @Test
  public void testCtorCopyWithConditionNotEquals() {
    QueryBuilder q1 = QueryBuilder.fromSqlQuery("SELECT * FROM t1");
    QueryBuilder copy = q1.copy();
    copy.withPartitionCondition("pary", "20180101", "20180201");

    Assert.assertNotEquals(q1.build(), copy.build());
  }

  @Test
  public void testCtorCopyWithLimitNotEquals() {
    QueryBuilder q1 = QueryBuilder.fromSqlQuery("SELECT * FROM t1");
    QueryBuilder copy = q1.copy();
    copy.withLimit(3L);

    Assert.assertNotEquals(q1.build(), copy.build());
  }

  @Test
  public void testCtorRawSqlWithWhere() {
    QueryBuilder wrapper = QueryBuilder.fromSqlQuery("SELECT * FROM t1 WHERE a > 100");

    String expected = "SELECT * FROM (SELECT * FROM t1 WHERE a > 100) WHERE 1=1";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testRawSqlWithLimit() {
    QueryBuilder wrapper = QueryBuilder.fromSqlQuery("SELECT * FROM t1");
    wrapper.withLimit(102L);

    String expected = "SELECT * FROM (SELECT * FROM t1) WHERE 1=1 LIMIT 102";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testRawSqlwithParallelization() {
    QueryBuilder wrapper = QueryBuilder.fromSqlQuery("SELECT * FROM t1");
    wrapper.withParallelizationCondition("bucket", 10, 20, true);

    String expected = "SELECT * FROM (SELECT * FROM t1) WHERE 1=1 AND bucket >= 10 AND bucket < 20";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testRawSqlWithPartition() {
    QueryBuilder wrapper = QueryBuilder.fromSqlQuery("SELECT * FROM t1");
    wrapper.withPartitionCondition("birthDate", "2018-01-01", "2018-02-01");

    String expected =
        "SELECT * FROM (SELECT * FROM t1) WHERE 1=1"
            + " AND birthDate >= '2018-01-01' AND birthDate < '2018-02-01'";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testRawSqlMultiline() {
    String sqlString =
            "SELECT a, b, c FROM t1\n"
            + " WHERE total > 100\n"
            + " AND country = 262\n";
    QueryBuilder wrapper = QueryBuilder.fromSqlQuery(sqlString);

    String expected =
        "SELECT * FROM (SELECT a, b, c FROM t1\n WHERE total > 100\n AND country = 262\n)"
            + " WHERE 1=1";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testRawSqlWithComments() {
    String sqlString =
        "-- We perform initial query here\n"
        + "SELECT a, b, c FROM t1\n" 
        + " WHERE total > 100";
    QueryBuilder wrapper = QueryBuilder.fromSqlQuery(sqlString);

    String expected =
        "SELECT * FROM ("
            + "-- We perform initial query here\nSELECT a, b, c FROM t1\n WHERE total > 100)"
            + " WHERE 1=1";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testRawSqlWithCte() {
    String sqlString =
        "WITH active_orders AS\n"
            + "(\n"
            + "    SELECT *\n"
            + "    FROM orders\n"
            + "    WHERE status = 'active' \n"
            + ")\n"
            + "SELECT date, SUM(amount)\n"
            + "FROM active_orders\n"
            + "GROUP BY date\n";
    QueryBuilder wrapper = QueryBuilder.fromSqlQuery(sqlString);

    String expected =
            "SELECT * FROM ("
            + "WITH active_orders AS\n"
            + "(\n"
            + "    SELECT *\n"
            + "    FROM orders\n"
            + "    WHERE status = 'active' \n"
            + ")\n"
            + "SELECT date, SUM(amount)\n"
            + "FROM active_orders\n"
            + "GROUP BY date\n"
            + ") WHERE 1=1";

    Assert.assertEquals(expected, wrapper.build());
  }

  
  
  @Test
  public void testItRemovesTrailingSemicolon() {
    List<String> rawInput =
        Arrays.asList(
            "SELECT * FROM coffees WHERE size > 10",
            "SELECT * FROM coffees WHERE size > 10;",
            "SELECT * FROM coffees WHERE size > 10 ",
            "SELECT * FROM coffees WHERE size > 10; ",
            "SELECT * FROM coffees WHERE size > 10 ;",
            "SELECT * FROM coffees WHERE size > 10\n;",
            "SELECT * FROM coffees WHERE size > 10\r;",
            "SELECT * FROM coffees WHERE size > 10\n\r;",
            "SELECT * FROM coffees WHERE size > 10;\n");
    List<String> expected =
        Arrays.asList(
            "SELECT * FROM (SELECT * FROM coffees WHERE size > 10) WHERE 1=1",
            "SELECT * FROM (SELECT * FROM coffees WHERE size > 10) WHERE 1=1",
            "SELECT * FROM (SELECT * FROM coffees WHERE size > 10 ) WHERE 1=1",
            "SELECT * FROM (SELECT * FROM coffees WHERE size > 10 ) WHERE 1=1",
            "SELECT * FROM (SELECT * FROM coffees WHERE size > 10 ) WHERE 1=1",
            "SELECT * FROM (SELECT * FROM coffees WHERE size > 10\n) WHERE 1=1",
            "SELECT * FROM (SELECT * FROM coffees WHERE size > 10\r) WHERE 1=1",
            "SELECT * FROM (SELECT * FROM coffees WHERE size > 10\n\r) WHERE 1=1",
            "SELECT * FROM (SELECT * FROM coffees WHERE size > 10\n) WHERE 1=1");

    for (int i = 0; i < rawInput.size(); i++) {
      execAndCompare(rawInput.get(i), expected.get(i));
    }
  }

  @Test
  public void testItGeneratesQueryForLimits() {
    String input = "SELECT * FROM coffees WHERE size > 10";
    String expected =
        "SELECT MIN(splitCol) as mixy, MAX(splitCol) as maxy "
            + "FROM (SELECT * FROM coffees WHERE size > 10)"
            + " WHERE 1=1 AND partition >= 'a' AND partition < 'd'";

    String actual =
        QueryBuilder.fromSqlQuery(input)
            .withPartitionCondition("partition", "a", "d")
            .generateQueryToGetLimitsOfSplitColumn("splitCol", "mixy", "maxy")
            .build();
    Assert.assertEquals(expected, actual);
  }

  private void execAndCompare(String rawInput, String expected) {
    String actual = QueryBuilder.fromSqlQuery(rawInput).build();

    Assert.assertEquals(expected, actual);
  }
}
