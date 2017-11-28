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

import com.spotify.scio.testing.PipelineSpec
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.scalatest.{BeforeAndAfterAll, Matchers}
import org.slf4j.LoggerFactory
import slick.jdbc.H2Profile.api._

import scala.concurrent.ExecutionContext.Implicits.global

class AvroToJdbcJobTest extends PipelineSpec with Matchers with BeforeAndAfterAll {
  private val logger = LoggerFactory.getLogger(classOf[AvroToJdbcJobTest])

  private val connectionUrl: String =
    "jdbc:h2:mem:test2;MODE=PostgreSQL;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1"
  private val db: Database = Database.forURL(connectionUrl, driver = "org.h2.Driver")
  private val inputAvro = new File(getClass.getResource("/avrotojdbctestdata.avro").getPath)

  override def beforeAll(): Unit = {
    AvroToJdbcTestFixtures.initializeEmptyDB(db)
  }

  "AvroToJdbcJob" should "work" in {

    JobTest[AvroToJdbcJob.type]
      .args(
        "--connectionUrl=" + connectionUrl,
        "--username=",
        "--password=",
        "--table=country",
        "--input=" + inputAvro.getAbsolutePath)
      .run()

    val datumReader = new GenericDatumReader[GenericRecord]
    val dataFileReader = new DataFileReader[GenericRecord](inputAvro, datumReader)
    var datum: GenericRecord = null
    while (dataFileReader.hasNext) {
      // Read one record from input Avro.
      datum = dataFileReader.next(datum)

      // Make sure each entry in Avro is found from the database, and only one per query.
      val query = for {
        c <- AvroToJdbcTestFixtures.country if c.code == datum.get("code")
      } yield (c.code, c.name)
      var foundOne = false
      db.stream(query.result).foreach {
        element =>
          require(!foundOne, "Multiple entries found for key: " + datum.get("code").toString)
          foundOne = true
          logger.info("{}", element)
      }
      require(foundOne, "Must have entry with key '" + datum.get("code").toString + "' in database")
    }

    db.run(AvroToJdbcTestFixtures.country.result).map(_.foreach {
      case (code, name, continent, region, surfaceArea, indepYear, population,
      lifeExpectancy, gnp, gnpold, localName, governmentForm, headOfState, capital, code2) =>
        logger.info("{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}",
          (code, name, continent, region, surfaceArea, indepYear, population, lifeExpectancy,
            gnp, gnpold, localName, governmentForm, headOfState, capital, code2))
    })
  }

}
