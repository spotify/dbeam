/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.dbeam

import org.apache.beam.sdk.PipelineResult
import org.apache.beam.sdk.metrics._

import scala.collection.JavaConverters._
import scala.util.Try

@SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf",
  "org.wartremover.warts.TraversableOps"))
object MetricsHelper {


  def getMetrics(result: PipelineResult): Map[String, Long] = {
    val metrics: MetricQueryResults = result.metrics().queryMetrics(MetricsFilter.builder().build())

    val gauges = metricsAtSteps(metrics.getGauges.asScala)
        .map{ case (k: MetricName, v: Map[String, MetricValue[GaugeResult]]) => (k.getName(),
        v.values.map(_.committed.getOrElse(GaugeResult.empty())).reduce(
          (x: GaugeResult, y: GaugeResult) =>
            if (x.getTimestamp isAfter y.getTimestamp) x else y).getValue
        )}
    val counters = metricsAtSteps(
      metrics.getCounters.asScala.asInstanceOf[Iterable[MetricResult[Long]]])
      .map{
        case (k: MetricName, v: Map[String, MetricValue[Long]]) =>
          (k.getName(), reduceMetricValues(v))}
    (gauges.toSeq ++ counters.toSeq).toMap
  }

  private def metricsAtSteps[T](results: Iterable[MetricResult[T]])
  : Map[MetricName, Map[String, MetricValue[T]]] =
    results
      .groupBy(_.getName())
      .mapValues { xs =>
        val m: Map[String, MetricValue[T]] = xs.map { r =>
          r.getStep -> MetricValue(r.getAttempted, Try(r.getCommitted).toOption)
        } (scala.collection.breakOut)
        m
      }

  private def reduceMetricValues(xs: Map[String, MetricValue[Long]]) = {
    xs.values.foldLeft(0L)( (t, m) => t + m.committed.getOrElse(0L))
  }

  final case class MetricValue[T](attempted: T, committed: Option[T])
}
