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
    final QueryBuilder wrapper = QueryBuilder.fromTablename("abc");

    final String expected = "SELECT * FROM abc WHERE 1=1";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testCtorRawSqlWithoutWhere() {
    final QueryBuilder wrapper = QueryBuilder.fromSqlQuery("SELECT * FROM t1");

    final String expected = "SELECT * FROM (SELECT * FROM t1) as user_sql_query WHERE 1=1";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testCtorCopyWithConditionNotEquals() {
    final QueryBuilder q1 = QueryBuilder.fromSqlQuery("SELECT * FROM t1");
    final QueryBuilder copy = q1.withPartitionCondition("pary", "20180101", "20180201");

    Assert.assertNotEquals(q1.build(), copy.build());
  }

  @Test
  public void testCtorCopyWithLimitNotEquals() {
    final QueryBuilder q1 = QueryBuilder.fromSqlQuery("SELECT * FROM t1");
    final QueryBuilder copy = q1.withLimit(3L);

    Assert.assertNotEquals(q1.build(), copy.build());
  }

  @Test
  public void testCtorRawSqlWithWhere() {
    final QueryBuilder wrapper = QueryBuilder.fromSqlQuery("SELECT * FROM t1 WHERE a > 100");

    final String expected =
        "SELECT * FROM (SELECT * FROM t1 WHERE a > 100) as user_sql_query WHERE 1=1";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testRawSqlWithLimit() {
    final QueryBuilder wrapper = QueryBuilder.fromSqlQuery("SELECT * FROM t1").withLimit(102L);

    final String expected =
        "SELECT * FROM (SELECT * FROM t1) as user_sql_query WHERE 1=1 LIMIT 102";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testRawSqlwithParallelization() {
    final QueryBuilder wrapper =
        QueryBuilder.fromSqlQuery("SELECT * FROM t1")
            .withParallelizationCondition("bucket", 10, 20, true);

    final String expected =
        "SELECT * FROM (SELECT * FROM t1) as user_sql_query"
            + " WHERE 1=1 AND bucket >= 10 AND bucket < 20";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testRawSqlWithPartition() {
    final QueryBuilder wrapper =
        QueryBuilder.fromSqlQuery("SELECT * FROM t1")
            .withPartitionCondition("birthDate", "2018-01-01", "2018-02-01");

    final String expected =
        "SELECT * FROM (SELECT * FROM t1) as user_sql_query WHERE 1=1"
            + " AND birthDate >= '2018-01-01' AND birthDate < '2018-02-01'";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testRawSqlMultiline() {
    final String sqlString =
        "SELECT a, b, c FROM t1\n" + " WHERE total > 100\n" + " AND country = 262\n";
    final QueryBuilder wrapper = QueryBuilder.fromSqlQuery(sqlString);

    String expected =
        "SELECT * FROM (SELECT a, b, c FROM t1\n WHERE total > 100\n AND country = 262\n)"
            + " as user_sql_query WHERE 1=1";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testRawSqlWithComments() {
    final String sqlString =
        "-- We perform initial query here\n" + "SELECT a, b, c FROM t1\n" + " WHERE total > 100";
    final QueryBuilder wrapper = QueryBuilder.fromSqlQuery(sqlString);

    final String expected =
        "SELECT * FROM ("
            + "-- We perform initial query here\nSELECT a, b, c FROM t1\n WHERE total > 100)"
            + " as user_sql_query WHERE 1=1";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testRawSqlWithCte() {
    final String sqlString =
        "WITH active_orders AS\n"
            + "(\n"
            + "    SELECT *\n"
            + "    FROM orders\n"
            + "    WHERE status = 'active' \n"
            + ")\n"
            + "SELECT date, SUM(amount)\n"
            + "FROM active_orders\n"
            + "GROUP BY date\n";
    final QueryBuilder wrapper = QueryBuilder.fromSqlQuery(sqlString);

    final String expected =
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
            + ") as user_sql_query WHERE 1=1";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testItRemovesTrailingSemicolon() {
    final List<String> rawInput =
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
    final List<String> expected =
        Arrays.asList(
            "SELECT * FROM (SELECT * FROM coffees WHERE size > 10) as user_sql_query WHERE 1=1",
            "SELECT * FROM (SELECT * FROM coffees WHERE size > 10) as user_sql_query WHERE 1=1",
            "SELECT * FROM (SELECT * FROM coffees WHERE size > 10 ) as user_sql_query WHERE 1=1",
            "SELECT * FROM (SELECT * FROM coffees WHERE size > 10 ) as user_sql_query WHERE 1=1",
            "SELECT * FROM (SELECT * FROM coffees WHERE size > 10 ) as user_sql_query WHERE 1=1",
            "SELECT * FROM (SELECT * FROM coffees WHERE size > 10\n) as user_sql_query WHERE 1=1",
            "SELECT * FROM (SELECT * FROM coffees WHERE size > 10\r) as user_sql_query WHERE 1=1",
            "SELECT * FROM (SELECT * FROM coffees WHERE size > 10\n\r) as user_sql_query WHERE 1=1",
            "SELECT * FROM (SELECT * FROM coffees WHERE size > 10\n) as user_sql_query WHERE 1=1");

    for (int i = 0; i < rawInput.size(); i++) {
      execAndCompare(rawInput.get(i), expected.get(i));
    }
  }

  @Test
  public void testItGeneratesQueryForLimits() {
    final String input = "SELECT * FROM coffees WHERE size > 10";
    final String expected =
        "SELECT MIN(splitCol) as mixy, MAX(splitCol) as maxy "
            + "FROM (SELECT * FROM coffees WHERE size > 10) as user_sql_query"
            + " WHERE 1=1 AND partition >= 'a' AND partition < 'd'";

    final String actual =
        QueryBuilder.fromSqlQuery(input)
            .withPartitionCondition("partition", "a", "d")
            .generateQueryToGetLimitsOfSplitColumn("splitCol", "mixy", "maxy")
            .build();
    Assert.assertEquals(expected, actual);
  }


  @Test
  public void testTableNameWithExcludedColumnsShouldStillUseSelectStar() {
    final com.google.common.collect.ImmutableSet<String> excludedColumns =
        com.google.common.collect.ImmutableSet.of("col1");
    final QueryBuilder wrapper =
        QueryBuilder.fromTablename("some_table")
            .withExcludedColumns(java.util.Optional.of(excludedColumns));

    final String expected = "SELECT * FROM some_table WHERE 1=1";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testRawSqlWithExcludedColumnShouldRemoveColumns() {
    final com.google.common.collect.ImmutableSet<String> excludedColumns =
        com.google.common.collect.ImmutableSet.of("col1");
    final QueryBuilder wrapper =
        QueryBuilder.fromSqlQuery("SELECT col1, col2 FROM some_table")
            .withExcludedColumns(java.util.Optional.of(excludedColumns));

    final String expected = 
        "SELECT * FROM (SELECT col2 FROM some_table) as user_sql_query WHERE 1=1";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testRawSqlWithFunctionAndAliasExcluded() {
    final com.google.common.collect.ImmutableSet<String> excludedColumns =
        com.google.common.collect.ImmutableSet.of("PSEUDO_PARTITION_ID");

    final QueryBuilder wrapper =
        QueryBuilder.fromSqlQuery(
                "SELECT t.*, MOD(id,2) AS PSEUDO_PARTITION_ID FROM demo_table t")
            .withSplitColumn(java.util.Optional.of("PSEUDO_PARTITION_ID"))
            .withExcludedColumns(java.util.Optional.of(excludedColumns));

    final String expected =
        "SELECT * FROM (SELECT t.*, MOD(id,2) AS PSEUDO_PARTITION_ID FROM demo_table t) "
            + "as user_sql_query WHERE 1=1";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testRawSqlWithCaseInsensitiveExcludedColumn() {
    final com.google.common.collect.ImmutableSet<String> excludedColumns =
        com.google.common.collect.ImmutableSet.of("COL1");
    final QueryBuilder wrapper =
        QueryBuilder.fromSqlQuery("SELECT col1, col2 FROM some_table")
            .withExcludedColumns(java.util.Optional.of(excludedColumns));

    final String expected =
        "SELECT * FROM (SELECT col2 FROM some_table) as user_sql_query WHERE 1=1";

    Assert.assertEquals(expected, wrapper.build());
  }

  @Test
  public void testRawSqlWithCaseInsensitiveSplitColumn() {
    final QueryBuilder wrapper =
        QueryBuilder.fromSqlQuery("SELECT col1, col2 FROM some_table")
            .withSplitColumn(java.util.Optional.of("COL1"));

    final String expected =
        "SELECT * FROM (SELECT col1, col2 FROM some_table) as user_sql_query WHERE 1=1";

    Assert.assertEquals(expected, wrapper.build());
  }

  private void execAndCompare(String rawInput, String expected) {
    final String actual = QueryBuilder.fromSqlQuery(rawInput).build();

    Assert.assertEquals(expected, actual);
  }
}
