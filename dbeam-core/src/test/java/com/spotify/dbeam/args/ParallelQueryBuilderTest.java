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

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.Lists;

import java.util.List;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;


public class ParallelQueryBuilderTest {

  private static String QUERY_BASE = "SELECT * FROM tab WHERE 1=1";
  private static QueryBuilder QUERY_FORMAT = QueryBuilder.fromTablename("tab");

  @Test
  public void shouldBuildParallelQueriesGivenRangeAndParallelism3() {
    final List<String> actual =
        ParallelQueryBuilder.queriesForBounds(100, 400, 3, "sp",
                                                                      QUERY_FORMAT);

    assertThat(
        actual,
        Matchers.is(
            Lists.newArrayList(
                format("%s AND sp >= %s AND sp < %s", QUERY_BASE, 100, 200),
                format("%s AND sp >= %s AND sp < %s", QUERY_BASE, 200, 300),
                format("%s AND sp >= %s AND sp <= %s", QUERY_BASE, 300, 400)
            ))
    );
  }

  @Test
  public void shouldBuildParallelQueriesGivenRangeThatDoesNotDivideEqually() {
    final List<String> actual =
        ParallelQueryBuilder.queriesForBounds(100, 402, 5, "sp",
                                              QUERY_FORMAT);

    assertThat(
        actual,
        Matchers.is(Lists.newArrayList(
            format("%s AND sp >= %s AND sp < %s", QUERY_BASE, 100, 161),
            format("%s AND sp >= %s AND sp < %s", QUERY_BASE, 161, 222),
            format("%s AND sp >= %s AND sp < %s", QUERY_BASE, 222, 283),
            format("%s AND sp >= %s AND sp < %s", QUERY_BASE, 283, 344),
            format("%s AND sp >= %s AND sp <= %s", QUERY_BASE, 344, 402)
        ))
    );
  }

  @Test
  public void shouldBuildSingleQueryWhenParallelismIsMoreThanMaxMin() {
    final List<String> actual =
        ParallelQueryBuilder.queriesForBounds(1, 2, 5, "sp",
                                              QUERY_FORMAT);

    Assert.assertThat(
        actual,
        Matchers.is(Lists.newArrayList(
            format("%s AND sp >= %s AND sp <= %s", QUERY_BASE, 1, 2)
        ))
    );
  }

  @Test
  public void shouldBuildSingleQueryWhenMaxMinIsTheSame() {
    final List<String> actual =
        ParallelQueryBuilder.queriesForBounds(1, 1, 5, "sp",
                                              QUERY_FORMAT);

    assertThat(
        actual,
        Matchers.is(Lists.newArrayList(
            format("%s AND sp >= %s AND sp <= %s", QUERY_BASE, 1, 1)
        ))
    );
  }

  @Test
  public void shouldBuildSingleQueryWhenParallelismIsOne() {
    final List<String> actual =
        ParallelQueryBuilder.queriesForBounds(1, 10, 1, "sp",
                                              QUERY_FORMAT);

    assertThat(
        actual,
        Matchers.is(Lists.newArrayList(
            format("%s AND sp >= %s AND sp <= %s", QUERY_BASE, 1, 10)
        ))
    );
  }

  @Ignore // TODO: fix this
  @Test
  public void shouldBuildMultipleQueriesWhenQueryingFromTwoRows() {
    final List<String> actual =
        ParallelQueryBuilder.queriesForBounds(1, 2, 2, "sp",
                                              QUERY_FORMAT);

    assertThat(
        actual,
        Matchers.is(Lists.newArrayList(
            format("%s AND sp >= %s AND sp < %s", QUERY_BASE, 1, 2),
            format("%s AND sp >= %s AND sp <= %s", QUERY_BASE, 2, 2)
        ))
    );
  }

}
