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

package com.spotify.dbeam.jobs

import com.spotify.dbeam.{JdbcTestFixtures, TestHelper}
import com.spotify.dbeam.avro.JdbcAvroMetering
import com.spotify.dbeam.options.OutputOptions
import java.io.File
import java.nio.file.{Files, Path}
import java.util
import java.util.stream.{Collectors, StreamSupport}
import java.util.Comparator

import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import slick.jdbc.H2Profile.api._

import scala.collection.JavaConverters._


@RunWith(classOf[JUnitRunner])
class JdbcAvroJobTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  private val connectionUrl: String =
    "jdbc:h2:mem:test2;MODE=PostgreSQL;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1"
  private val db: Database = Database.forURL(connectionUrl, driver = "org.h2.Driver")
  private val dir = TestHelper.createTmpDirName("jdbc-avro-test-").toFile
  private val passwordFile = new File(dir.getAbsolutePath + ".password")
  

  override def beforeAll(): Unit = {
    JdbcTestFixtures.createFixtures(db, Seq(JdbcTestFixtures.record1, JdbcTestFixtures.record2))
    passwordFile.createNewFile()
  }

  override protected def afterAll(): Unit = {
    passwordFile.delete()
    Files.walk(dir.toPath)
      .sorted(Comparator.reverseOrder())
      .iterator()
      .asScala
      .foreach((p: Path) => p.toFile.delete())
  }

  "JdbcAvroJob" should "work" in {
    JdbcAvroJob.main(Array(
        "--targetParallelism=1",  // no need for more threads when testing
        "--partition=2025-02-28",
        "--skipPartitionCheck",
        "--exportTimeout=PT1M",
        "--connectionUrl=" + connectionUrl,
        "--username=",
        "--passwordFile=" + passwordFile.getAbsolutePath,
        "--table=COFFEES",
        "--output=" + dir.getAbsolutePath,
        "--avroCodec=deflate1"
    ))
    val files: Array[File] = dir.listFiles()
    files.map(_.getName) should contain theSameElementsAs Seq(
      "_AVRO_SCHEMA.avsc", "_METRICS.json", "_SERVICE_METRICS.json",
      "_queries", "part-00000-of-00001.avro")
    files.filter(_.getName.equals("_queries"))(0).listFiles().map(_.getName) should
      contain theSameElementsAs Seq("query_0.sql")
    val schema = new Schema.Parser().parse(new File(dir, "_AVRO_SCHEMA.avsc"))
    val records: util.List[GenericRecord] =
      readAvroRecords(new File(dir, "part-00000-of-00001.avro"), schema)
    records should have size 2
  }

  private def readAvroRecords(avroFile: File, schema: Schema): util.List[GenericRecord] = {
    val datum: GenericDatumReader[GenericRecord] = new GenericDatumReader(schema)
    val dataFileReader = new DataFileReader(avroFile, datum)
    val records: util.List[GenericRecord] = StreamSupport.stream(dataFileReader.spliterator(), false)
                           .collect(Collectors.toList())
    dataFileReader.close()
    records
  }

  "JdbcAvroJob" should "have a default exit code" in {
    ExceptionHandling.exitCode(new IllegalStateException()) should be (49)
  }

  "JdbcAvroJob" should "fail on missing input" in {
    val pipelineOptions = PipelineOptionsFactory.create()
    a[IllegalArgumentException] should be thrownBy {
      JdbcAvroJob.create(pipelineOptions)
    }
  }

  "JdbcAvroJob" should "fail on empty input" in {
    val pipelineOptions = PipelineOptionsFactory.create()
    pipelineOptions.as(classOf[OutputOptions]).setOutput("")
    a[IllegalArgumentException] should be thrownBy {
      JdbcAvroJob.create(pipelineOptions)
    }
  }

  "JdbcAvroJob" should "increment counter metrics" in {
    val metering = new JdbcAvroMetering(1, 1)
    metering.exposeWriteElapsedMs(0)
    metering.incrementRecordCount()
    metering.exposeWriteElapsedMs(0)
  }

}
