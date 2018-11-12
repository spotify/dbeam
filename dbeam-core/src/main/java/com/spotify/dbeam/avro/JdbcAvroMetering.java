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

package com.spotify.dbeam.avro;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcAvroMetering {
  private static final int COUNTER_REPORT_EVERY = 100000;
  private static final int LOG_EVERY = 100000;
  private final Logger logger = LoggerFactory.getLogger(JdbcAvroMetering.class);
  private Counter recordCount =
      Metrics.counter(this.getClass().getCanonicalName(), "recordCount");
  private Counter executeQueryElapsedMs =
      Metrics.counter(this.getClass().getCanonicalName(), "executeQueryElapsedMs");
  private Counter writeElapsedMs =
      Metrics.counter(this.getClass().getCanonicalName(), "writeElapsedMs");
  private Gauge msPerMillionRows =
      Metrics.gauge(this.getClass().getCanonicalName(), "msPerMillionRows");
  private Gauge rowsPerMinute =
      Metrics.gauge(this.getClass().getCanonicalName(), "rowsPerMinute");
  private int rowCount = 0;
  private long writeIterateStartTime;

  /**
   * Increment and report counters to Beam SDK and logs.
   * To avoid slowing down the writes, counts are reported every x 1000s of rows.
   * This exposes the job progress.
   */
  public void incrementRecordCount() {
    this.rowCount++;
    if ((this.rowCount % COUNTER_REPORT_EVERY) == 0) {
      this.recordCount.inc(COUNTER_REPORT_EVERY);
      long elapsedMs = System.currentTimeMillis() - this.writeIterateStartTime;
      long msPerMillionRows = 1000000L * elapsedMs / rowCount;
      long rowsPerMinute = (60 * 1000L) * rowCount / elapsedMs;
      this.msPerMillionRows.set(msPerMillionRows);
      this.rowsPerMinute.set(rowsPerMinute);
      if ((this.rowCount % LOG_EVERY) == 0) {
        logger.info(String.format(
            "jdbcavroio : Fetched # %08d rows at %08d rows per minute and %08d ms per M rows",
            rowCount, rowsPerMinute, msPerMillionRows));
      }
    }
  }

  public void exposeMetrics(long elapsedMs) {
    logger.info(String.format("jdbcavroio : Read %d rows, took %5.2f seconds",
                              rowCount, elapsedMs / 1000.0));
    this.writeElapsedMs.inc(elapsedMs);
    if (rowCount > 0) {
      this.recordCount.inc((this.rowCount % COUNTER_REPORT_EVERY));
      this.msPerMillionRows.set(1000000L * elapsedMs / rowCount);
      if (elapsedMs != 0) {
        this.rowsPerMinute.set((60 * 1000L) * rowCount / elapsedMs);
      }
    }
  }

  public void startIterate() {
    this.writeIterateStartTime = System.currentTimeMillis();
  }

  public void finishIterate() {
    exposeMetrics(System.currentTimeMillis() - this.writeIterateStartTime);
  }

  public void finishExecuteQuery(long elapsed) {
    logger.info(String.format("jdbcavroio : Execute query took %5.2f seconds",
                              elapsed / 1000.0));
    this.executeQueryElapsedMs.inc(elapsed);
  }
}
