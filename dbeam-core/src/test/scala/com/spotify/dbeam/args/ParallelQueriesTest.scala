/*
 * Copyright 2017-2019 Spotify AB.
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

package com.spotify.dbeam.args

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

/**
 * Additional tests for building parallel queries when splitColumn
 * and queryParallelism is specified.
 */
@RunWith(classOf[JUnitRunner])
class ParallelQueriesTest extends FlatSpec with Matchers {

  private val tablename = "tab"
  private val queryFormat = "SELECT * FROM tab WHERE 1=1"

  private def splitPointsToRanges(splitPoints: Seq[Long]): List[String] = {
    val paired = splitPoints
      .sorted
      .sliding(2)
      .toList

    paired
      .reverse
      .tail
      .reverse.map(z => String.format(queryFormat+"%s", s" AND sp >= ${z(0)} AND sp < ${z(1)}")) ++
      paired
        .lastOption
        .map(z => String.format(queryFormat+"%s", s" AND sp >= ${z(0)} AND sp <= ${z(1)}")) // last element should be included
  }

  private def queriesForBounds2(
      min: Long, max: Long, parallelism: Int, splitColumn: String, queryFormat: String): java.util.List[String] = {
    val queries = QueryBuilderArgs.queriesForBounds(min, max, parallelism, splitColumn, QueryBuilder.fromTablename(tablename))
    val q2 = queries.asScala.map(x => x.toString()).toList.asJava
    q2
  }
  
  it should "build appropriate parallel queries for a a given range" in {
    val actual = queriesForBounds2(100, 400, 3, "sp", queryFormat)
    val expected = splitPointsToRanges(Seq(100, 200, 300, 400))

    actual.asScala should contain theSameElementsAs expected
  }

  it should "build appropriate parallel queries for a a given range which doesn't divide equally" in {
    val actual = queriesForBounds2(100, 400, 7, "sp", queryFormat)
    val expected = splitPointsToRanges(Seq(100, 143, 186, 229, 272, 315, 358, 400))

    actual.asScala should contain theSameElementsAs expected
  }

  it should "parallel queries should have equal distribution of range" in {
    val actual = queriesForBounds2(100, 400, 6, "sp", queryFormat)
    val expected = splitPointsToRanges(Seq(100, 150, 200, 250, 300, 350, 400))

    actual.asScala should contain theSameElementsAs expected
  }

  it should "build 1 query if queryParallelism is more than max-min" in {
    val actual = queriesForBounds2(1, 2, 5, "sp", queryFormat)
    val expected = splitPointsToRanges(Seq(1, 2))

    actual.asScala should contain theSameElementsAs expected
  }

  it should "build 1 query when max and min are same" in {
    val actual = queriesForBounds2(1, 1, 5, "sp", queryFormat)
    val expected = splitPointsToRanges(Seq(1, 1))

    actual.asScala should contain theSameElementsAs expected
  }

  it should "build 1 query when max and min are same and queryParallelism is 1" in {
    val actual = queriesForBounds2(1, 1, 1, "sp", queryFormat)
    val expected = splitPointsToRanges(Seq(1, 1))

    actual.asScala should contain theSameElementsAs expected
  }

  it should "build 1 query when queryParallelism is 1" in {
    val actual = queriesForBounds2(2, 345, 1, "sp", queryFormat)
    val expected = splitPointsToRanges(Seq(2, 345))

    actual.asScala should contain theSameElementsAs expected
  }

}
