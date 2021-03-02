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

  private final int countReportEvery;
  private final int logEvery;
  private final Logger logger = LoggerFactory.getLogger(JdbcAvroMetering.class);
  private Counter recordCount = Metrics.counter(this.getClass().getCanonicalName(), "recordCount");
  private Counter executeQueryElapsedMs =
      Metrics.counter(this.getClass().getCanonicalName(), "executeQueryElapsedMs");
  private Counter writeElapsedMs =
      Metrics.counter(this.getClass().getCanonicalName(), "writeElapsedMs");
  private Gauge msPerMillionRows =
      Metrics.gauge(this.getClass().getCanonicalName(), "msPerMillionRows");
  private Gauge rowsPerMinute = Metrics.gauge(this.getClass().getCanonicalName(), "rowsPerMinute");
  private Counter bytesWritten =
      Metrics.counter(this.getClass().getCanonicalName(), "bytesWritten");
  private long rowCount = 0;
  private long writeIterateStartTime;

  public JdbcAvroMetering(int countReportEvery, int logEvery) {
    this.countReportEvery = countReportEvery;
    this.logEvery = logEvery;
  }

  public static JdbcAvroMetering create() {
    return new JdbcAvroMetering(100000, 100000);
  }

  /**
   * Increment and report counters to Beam SDK and logs. To avoid slowing down the writes, counts
   * are reported every x 1000s of rows. This exposes the job progress.
   */
  public void incrementRecordCount() {
    this.rowCount++;
    if ((this.rowCount % countReportEvery) == 0) {
      this.recordCount.inc(countReportEvery);
      final long elapsedNano = System.nanoTime() - this.writeIterateStartTime;
      final long msPerMillionRows = elapsedNano / rowCount;
      final long rowsPerMinute = (60 * 1000000000L) * rowCount / elapsedNano;
      this.msPerMillionRows.set(msPerMillionRows);
      this.rowsPerMinute.set(rowsPerMinute);
      if ((this.rowCount % logEvery) == 0) {
        logger.info(
            String.format(
                "jdbcavroio : Fetched # %08d rows at %08d rows per minute and %08d ms per M rows",
                rowCount, rowsPerMinute, msPerMillionRows));
      }
    }
  }

  public void exposeWriteElapsed() {
    long elapsedMs = (System.nanoTime() - this.writeIterateStartTime) / 1000000L;
    logger.info("jdbcavroio : Read {} rows, took {} seconds", rowCount, elapsedMs / 1000.0);
    this.writeElapsedMs.inc(elapsedMs);
    if (rowCount > 0) {
      this.recordCount.inc((this.rowCount % countReportEvery));
      this.msPerMillionRows.set(1000000L * elapsedMs / rowCount);
      if (elapsedMs != 0) {
        this.rowsPerMinute.set((60 * 1000L) * rowCount / elapsedMs);
      }
    }
  }

  public long startWriteMeter() {
    long startTs = System.nanoTime();
    this.writeIterateStartTime = startTs;
    this.rowCount = 0;
    return startTs;
  }

  public void exposeExecuteQueryMs(final long elapsedMs) {
    logger.info("jdbcavroio : Execute query took {} seconds", elapsedMs / 1000.0);
    this.executeQueryElapsedMs.inc(elapsedMs);
  }

  public void exposeWrittenBytes(final long count) {
    this.bytesWritten.inc(count);
  }
}
