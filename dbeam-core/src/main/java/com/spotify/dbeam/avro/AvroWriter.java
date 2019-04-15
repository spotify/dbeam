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

package com.spotify.dbeam.avro;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes ByteBuffer datums from a BlockingQueue and writes to a DataFileWriter.
 */
public class AvroWriter implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(AvroWriter.class);
  private final DataFileWriter<GenericRecord> dataFileWriter;
  private final JdbcAvroMetering metering;
  private final BlockingQueue<ByteBuffer> queue;

  public AvroWriter(
      DataFileWriter<GenericRecord> dataFileWriter,
      JdbcAvroMetering metering,
      BlockingQueue<ByteBuffer> queue) {
    this.dataFileWriter = dataFileWriter;
    this.metering = metering;
    this.queue = queue;
  }

  @Override
  public void run() {
    try {
      while (true) {
        final ByteBuffer datum = queue.take();
        if (datum.capacity() == 0) {
          this.dataFileWriter.sync();
          return;
        } else {
          this.dataFileWriter.appendEncoded(datum);
        }
      }
    } catch (InterruptedException ex) {
      System.out.println("CONSUMER INTERRUPTED");
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
