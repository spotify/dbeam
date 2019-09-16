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

package com.spotify.dbeam.avro

import java.io.ByteArrayOutputStream
import java.util.Optional

import com.spotify.dbeam.{Coffee, DbTestHelper, TestHelper}
import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, DataFileWriter, SeekableByteArrayInput}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class JdbcAvroRecordTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  private val connectionUrl: String =
    "jdbc:h2:mem:test;MODE=PostgreSQL;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1"
  private val record1 = Coffee.COFFEE1

  override def beforeAll(): Unit = {
    DbTestHelper.createFixtures(connectionUrl)
  }

  it should "create schema" in {
    val fieldCount = 12
    val actual: Schema = JdbcAvroSchema.createSchemaByReadingOneRow(
      DbTestHelper.createConnection(connectionUrl),
      "COFFEES", "dbeam_generated",
      "Generate schema from JDBC ResultSet from COFFEES jdbc:h2:mem:test", false)

    actual shouldNot be (null)
    actual.getNamespace should be ("dbeam_generated")
    actual.getProp("tableName") should be ("COFFEES")
    actual.getProp("connectionUrl") should be ("jdbc:h2:mem:test")
    actual.getFields.size() should be (fieldCount)
    actual.getFields.asScala.map(_.name()) should
      be (List("COF_NAME", "SUP_ID", "PRICE", "TEMPERATURE", "SIZE",
               "IS_ARABIC", "SALES", "TOTAL", "CREATED", "UPDATED", "UID", "ROWNUM"))
    actual.getFields.asScala.map(_.schema().getType) should
      be (List.fill(fieldCount)(Schema.Type.UNION))
    actual.getFields.asScala.map(_.schema().getTypes.get(0).getType) should
      be (List.fill(fieldCount)(Schema.Type.NULL))
    actual.getFields.asScala.map(_.schema().getTypes.size()) should be (List.fill(fieldCount)(2))
    actual.getField("COF_NAME").schema().getTypes.get(1).getType should be (Schema.Type.STRING)
    actual.getField("SUP_ID").schema().getTypes.get(1).getType should be (Schema.Type.INT)
    actual.getField("PRICE").schema().getTypes.get(1).getType should be (Schema.Type.STRING)
    actual.getField("TEMPERATURE").schema().getTypes.get(1).getType should be (Schema.Type.FLOAT)
    actual.getField("SIZE").schema().getTypes.get(1).getType should be (Schema.Type.DOUBLE)
    actual.getField("IS_ARABIC").schema().getTypes.get(1).getType should be (Schema.Type.BOOLEAN)
    actual.getField("SALES").schema().getTypes.get(1).getType should be (Schema.Type.INT)
    actual.getField("TOTAL").schema().getTypes.get(1).getType should be (Schema.Type.LONG)
    actual.getField("CREATED").schema().getTypes.get(1).getType should be (Schema.Type.LONG)
    actual.getField("UPDATED").schema().getTypes.get(1).getType should be (Schema.Type.LONG)
    actual.getField("UPDATED").schema().getTypes.get(1).getProp("logicalType") should be (null)
    actual.getField("UID").schema().getTypes.get(1).getType should be (Schema.Type.BYTES)
    actual.getField("ROWNUM").schema().getTypes.get(1).getType should be (Schema.Type.LONG)
    actual.toString shouldNot be (null)
    actual.getDoc should be ("Generate schema from JDBC ResultSet from COFFEES jdbc:h2:mem:test")
  }

  it should "create schema with logical types" in {
    val fieldCount = 12
    val actual: Schema = JdbcAvroSchema.createSchemaByReadingOneRow(
      DbTestHelper.createConnection(connectionUrl),
      "COFFEES", "dbeam_generated",
      "Generate schema from JDBC ResultSet from COFFEES jdbc:h2:mem:test",
      true)

    actual shouldNot be (null)
    actual.getNamespace should be ("dbeam_generated")
    actual.getProp("tableName") should be ("COFFEES")
    actual.getProp("connectionUrl") should be ("jdbc:h2:mem:test")
    actual.getFields.size() should be (fieldCount)
    actual.getFields.asScala.map(_.name()) should
      be (List("COF_NAME", "SUP_ID", "PRICE", "TEMPERATURE", "SIZE",
        "IS_ARABIC", "SALES", "TOTAL", "CREATED", "UPDATED", "UID", "ROWNUM"))
    actual.getFields.asScala.map(_.schema().getType) should
      be (List.fill(fieldCount)(Schema.Type.UNION))
    actual.getFields.asScala.map(_.schema().getTypes.get(0).getType) should
      be (List.fill(fieldCount)(Schema.Type.NULL))
    actual.getFields.asScala.map(_.schema().getTypes.size()) should be (List.fill(fieldCount)(2))
    actual.getField("CREATED").schema().getTypes.get(1).getType should be (Schema.Type.LONG)
    actual.getField("UPDATED").schema().getTypes.get(1).getType should be (Schema.Type.LONG)
    actual.getField("UPDATED").schema().getTypes.get(1).getProp("logicalType") should
      be ("timestamp-millis")
    actual.getField("UID").schema().getTypes.get(1).getType should be (Schema.Type.BYTES)
    actual.getField("ROWNUM").schema().getTypes.get(1).getType should be (Schema.Type.LONG)
    actual.toString shouldNot be (null)
    actual.getDoc should be ("Generate schema from JDBC ResultSet from COFFEES jdbc:h2:mem:test")
  }

  it should "create schema with specified namespace and doc string" in {
    val actual: Schema = JdbcAvroSchema.createSchemaByReadingOneRow(
      DbTestHelper.createConnection(connectionUrl),
      "COFFEES", "ns", "doc", false)

    actual shouldNot be (null)
    actual.getNamespace should be ("ns")
    actual.getDoc should be ("doc")
  }

  it should "encode jdbc result set to valid avro" in {
    val rs = DbTestHelper.createConnection(connectionUrl).createStatement().executeQuery(s"SELECT * FROM COFFEES")
    val schema = JdbcAvroSchema.createAvroSchema(rs, "dbeam_generated","connection", "doc", false)
    val converter = JdbcAvroRecordConverter.create(rs)
    val dataFileWriter = new DataFileWriter(new GenericDatumWriter[GenericRecord](schema))
    val outputStream = new ByteArrayOutputStream()
    dataFileWriter.create(schema, outputStream)
    // convert and write
    while (rs.next()) {
      dataFileWriter.appendEncoded(converter.convertResultSetIntoAvroBytes())
    }
    dataFileWriter.flush()
    outputStream.close()
    // transform to generic record
    val inputStream = new SeekableByteArrayInput(outputStream.toByteArray)
    val dataFileReader = new DataFileReader[GenericRecord](inputStream,
      new GenericDatumReader[GenericRecord](schema))
    val records: Seq[GenericRecord] =
      dataFileReader.asInstanceOf[java.lang.Iterable[GenericRecord]].asScala.toSeq
    records.size should be (2)
    val record: GenericRecord = records.filter(r => r.get(0).toString == record1.name()).head

    record shouldNot be (null)
    record.getSchema should be (schema)
    record.getSchema.getFields.size() should be (12)
    val actual = Coffee.create(
      record.get(0).toString,
      Optional.ofNullable(record.get(1).asInstanceOf[java.lang.Integer]),
      new java.math.BigDecimal(record.get(2).toString),
      record.get(3).asInstanceOf[java.lang.Float],
      record.get(4).asInstanceOf[java.lang.Double],
      record.get(5).asInstanceOf[java.lang.Boolean],
      record.get(6).asInstanceOf[java.lang.Integer],
      record.get(7).asInstanceOf[java.lang.Long],
      new java.sql.Timestamp(record.get(8).asInstanceOf[java.lang.Long]),
      Optional.ofNullable(Option(record.get(9).asInstanceOf[java.lang.Long]).map(v => new java.sql.Timestamp(v)).orNull),
      TestHelper.byteBufferToUuid(record.get(10).asInstanceOf[java.nio.ByteBuffer]),
      record.get(11).asInstanceOf[java.lang.Long]
    )
    actual should be (record1)
  }

}
