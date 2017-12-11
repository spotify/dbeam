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

import java.sql.Connection

import com.spotify.dbeam.options.JdbcImportArgs
import com.spotify.scio.{ScioContext, ScioResult}
import org.apache.avro.generic.GenericRecord
import org.slf4j.{Logger, LoggerFactory}

object AvroToJdbcJob {
  val log: Logger = LoggerFactory.getLogger(AvroToJdbcJob.getClass)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc: ScioContext, args: JdbcImportArgs) = JdbcImportArgs.contextAndArgs(cmdlineArgs)
    log.info("AvroToJdbcJob with input: " + args.input)

    // val conn: Connection = args.createConnection()

    val avroFilePath: String = args.input
    sc.avroFile[GenericRecord](avroFilePath).applyTransform(new AvroToJdbcTransform())

    val scioResult: ScioResult = sc.close().waitUntilDone()
  }
}
