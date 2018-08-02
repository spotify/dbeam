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

import java.io.File
import java.nio.file.{Files, Path}
import java.util
import java.util.{Comparator, UUID}

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.AvroSource
import org.apache.beam.sdk.testing.SourceTestUtils
import org.scalatest._
import slick.jdbc.H2Profile.api._

import scala.collection.JavaConverters._

class JdbcAvroJobTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  private val connectionUrl: String =
    "jdbc:h2:mem:test2;MODE=PostgreSQL;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1"
  private val db: Database = Database.forURL(connectionUrl, driver = "org.h2.Driver")
  private val dir = tmpDir

  def tmpDir: File = new File(
    new File(sys.props("java.io.tmpdir")),
    "jdbc-avro-test-" + UUID.randomUUID().toString)

  override def beforeAll(): Unit = {
    JdbcTestFixtures.createFixtures(db, Seq(JdbcTestFixtures.record1, JdbcTestFixtures.record2))
  }

  override protected def afterAll(): Unit = {
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
        "--connectionUrl=" + connectionUrl,
        "--username=",
        "--password=",
        "--table=coffees",
        "--output=" + dir.getAbsolutePath)
    )
    val files: Array[File] = dir.listFiles()
    files.map(_.getName) should contain theSameElementsAs Seq(
      "_AVRO_SCHEMA.avsc", "_METRICS.json", "_SERVICE_METRICS.json",
      "_queries", "part-00000-of-00001.avro")
    files.filter(_.getName.equals("_queries"))(0).listFiles().map(_.getName) should
      contain theSameElementsAs Seq("query_0.sql")
    val schema = new Schema.Parser().parse(new File(dir, "_AVRO_SCHEMA.avsc"))
    val source: AvroSource[GenericRecord] = AvroSource
      .from(new File(dir, "part-00000-of-00001.avro").toString)
      .withSchema(schema)
    val records: util.List[GenericRecord] = SourceTestUtils.readFromSource(source, null)
    records should have size 2
  }

}
