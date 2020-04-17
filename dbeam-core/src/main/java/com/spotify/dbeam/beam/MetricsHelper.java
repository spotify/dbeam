/*-
 * -\-\-
 * DBeam Core
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
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

package com.spotify.dbeam.beam;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;

public class MetricsHelper {

  private static final Function<MetricResult<GaugeResult>, GaugeResult> GET_COMMITTED_GAUGE =
      metricResult -> {
        try {
          return metricResult.getCommitted();
        } catch (UnsupportedOperationException e) {
          return GaugeResult.empty();
        }
      };

  public static final ToLongFunction<MetricResult<Long>> GET_COMMITTED_COUNTER =
      metricResult -> {
        try {
          return metricResult.getCommitted();
        } catch (UnsupportedOperationException e) {
          return 0L;
        }
      };

  public static Map<String, Long> getMetrics(final PipelineResult result) {
    final MetricQueryResults metricQueryResults =
        result.metrics().queryMetrics(MetricsFilter.builder().build());

    final Map<String, Long> gauges =
        StreamSupport.stream(metricQueryResults.getGauges().spliterator(), false)
            .collect(
                Collectors.groupingBy(
                    MetricResult::getName,
                    Collectors.reducing(
                        GaugeResult.empty(),
                        GET_COMMITTED_GAUGE,
                        BinaryOperator.maxBy(Comparator.comparing(GaugeResult::getTimestamp)))))
            .entrySet()
            .stream()
            .collect(Collectors.toMap(e -> e.getKey().getName(), e -> e.getValue().getValue()));

    final Map<String, Long> counters =
        StreamSupport.stream(metricQueryResults.getCounters().spliterator(), false)
            .collect(
                Collectors.groupingBy(
                    m -> m.getName().getName(), Collectors.summingLong(GET_COMMITTED_COUNTER)));
    Map<String, Long> ret = new HashMap<>();
    ret.putAll(gauges);
    ret.putAll(counters);
    addCalculatedMetrics(counters, ret);
    return Collections.unmodifiableMap(ret);
  }

  private static void addCalculatedMetrics(final Map<String, Long> counters,
                                           final Map<String, Long> ret) {
    // calculate and add KBps
    final Long writeElapsedMs = counters.get("writeElapsedMs");
    final Long kbps;
    if (writeElapsedMs != null && writeElapsedMs > 0) {
      kbps = counters.get("bytesWritten") / writeElapsedMs;
    } else {
      kbps = -1L;
    }
    ret.put("KbWritePerSec", kbps);
  }
}
