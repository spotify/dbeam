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

import com.google.common.collect.Lists;
import com.spotify.dbeam.Coffee;
import com.spotify.dbeam.DbTestHelper;
import com.spotify.dbeam.TestHelper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class JdbcAvroRecordTest {

  private static String CONNECTION_URL =
      "jdbc:h2:mem:test;MODE=PostgreSQL;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1";

  @BeforeClass
  public static void beforeAll() throws SQLException, ClassNotFoundException {
    DbTestHelper.createFixtures(CONNECTION_URL);
  }

  @Test
  public void shouldCreateSchema() throws ClassNotFoundException, SQLException {
    int fieldCount = 12;
    Schema actual = JdbcAvroSchema.createSchemaByReadingOneRow(
        DbTestHelper.createConnection(CONNECTION_URL),
        "COFFEES", "dbeam_generated",
        "Generate schema from JDBC ResultSet from COFFEES jdbc:h2:mem:test", false);

    Assert.assertNotNull(actual);
    Assert.assertEquals("dbeam_generated", actual.getNamespace());
    Assert.assertEquals("COFFEES", actual.getProp("tableName"));
    Assert.assertEquals("jdbc:h2:mem:test", actual.getProp("connectionUrl"));
    Assert.assertEquals(
        "Generate schema from JDBC ResultSet from COFFEES jdbc:h2:mem:test",
        actual.getDoc());
    Assert.assertEquals(fieldCount, actual.getFields().size());
    Assert.assertEquals(Lists.newArrayList(
        "COF_NAME", "SUP_ID", "PRICE", "TEMPERATURE", "SIZE",
        "IS_ARABIC", "SALES", "TOTAL", "CREATED", "UPDATED", "UID", "ROWNUM"
    ), actual.getFields().stream().map(Schema.Field::name).collect(Collectors.toList()));
    for (Schema.Field f: actual.getFields()) {
      Assert.assertEquals(Schema.Type.UNION, f.schema().getType());
      Assert.assertEquals(2, f.schema().getTypes().size());
      Assert.assertEquals(Schema.Type.NULL, f.schema().getTypes().get(0).getType());
    }
    Assert.assertEquals(Schema.Type.STRING,
                        actual.getField("COF_NAME").schema().getTypes().get(1).getType());
    Assert.assertEquals(Schema.Type.INT,
                        actual.getField("SUP_ID").schema().getTypes().get(1).getType());
    Assert.assertEquals(Schema.Type.STRING,
                        actual.getField("PRICE").schema().getTypes().get(1).getType());
    Assert.assertEquals(Schema.Type.FLOAT,
                        actual.getField("TEMPERATURE").schema().getTypes().get(1).getType());
    Assert.assertEquals(Schema.Type.DOUBLE,
                        actual.getField("SIZE").schema().getTypes().get(1).getType());
    Assert.assertEquals(Schema.Type.BOOLEAN,
                        actual.getField("IS_ARABIC").schema().getTypes().get(1).getType());
    Assert.assertEquals(Schema.Type.INT,
                        actual.getField("SALES").schema().getTypes().get(1).getType());
    Assert.assertEquals(Schema.Type.LONG,
                        actual.getField("TOTAL").schema().getTypes().get(1).getType());
    Assert.assertEquals(Schema.Type.LONG,
                        actual.getField("CREATED").schema().getTypes().get(1).getType());
    Assert.assertEquals(Schema.Type.LONG,
                        actual.getField("UPDATED").schema().getTypes().get(1).getType());
    Assert.assertEquals(Schema.Type.BYTES,
                        actual.getField("UID").schema().getTypes().get(1).getType());
    Assert.assertEquals(Schema.Type.LONG,
                        actual.getField("ROWNUM").schema().getTypes().get(1).getType());
    Assert.assertNull(actual.getField("UPDATED").schema()
                          .getTypes().get(1).getProp("logicalType"));
  }

  @Test
  public void shouldCreateSchemaWithLogicalTypes() throws ClassNotFoundException, SQLException {
    int fieldCount = 12;
    Schema actual = JdbcAvroSchema.createSchemaByReadingOneRow(
        DbTestHelper.createConnection(CONNECTION_URL),
        "COFFEES", "dbeam_generated",
        "Generate schema from JDBC ResultSet from COFFEES jdbc:h2:mem:test", true);

    Assert.assertEquals(fieldCount, actual.getFields().size());
    Assert.assertEquals("timestamp-millis",
                        actual.getField("UPDATED").schema()
                            .getTypes().get(1).getProp("logicalType"));
  }

  @Test
  public void shouldEncodeResultSetToValidAvro()
      throws ClassNotFoundException, SQLException, IOException {
    ResultSet rs = DbTestHelper.createConnection(CONNECTION_URL)
        .createStatement().executeQuery("SELECT * FROM COFFEES");
    Schema schema =
        JdbcAvroSchema.createAvroSchema(
            rs, "dbeam_generated", "connection", "doc", false, Optional.empty());
    JdbcAvroRecordConverter converter = JdbcAvroRecordConverter.create(rs);
    DataFileWriter<GenericRecord> dataFileWriter =
        new DataFileWriter<>(new GenericDatumWriter<>(schema));
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    dataFileWriter.create(schema, outputStream);
    // convert and write
    while (rs.next()) {
      dataFileWriter.appendEncoded(converter.convertResultSetIntoAvroBytes());
    }
    dataFileWriter.flush();
    outputStream.close();
    // transform to generic record
    SeekableByteArrayInput inputStream = new SeekableByteArrayInput(outputStream.toByteArray());
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(inputStream,
                                                 new GenericDatumReader<>(
                                                     schema));
    final List<GenericRecord> records =
        StreamSupport.stream(dataFileReader.spliterator(), false).collect(Collectors.toList());

    Assert.assertEquals(2, records.size());
    GenericRecord record = records.stream()
        .filter(r -> Coffee.COFFEE1.name().equals(r.get(0).toString()))
        .findFirst().orElseThrow(() -> new IllegalArgumentException("not found"));

    Assert.assertEquals(12, record.getSchema().getFields().size());
    Assert.assertEquals(schema, record.getSchema());
    Coffee actual = Coffee.create(
        record.get(0).toString(),
        Optional.ofNullable((Integer) record.get(1)),
        new java.math.BigDecimal(record.get(2).toString()),
        (Float)record.get(3),
        (Double)record.get(4),
        (Boolean)record.get(5),
        (Integer)record.get(6),
        (Long)record.get(7),
        new java.sql.Timestamp((Long)record.get(8)),
        Optional.ofNullable((Long)record.get(9)).map(Timestamp::new),
        TestHelper.byteBufferToUuid((ByteBuffer)record.get(10)),
        (Long)record.get(11)
    );
    Assert.assertEquals(Coffee.COFFEE1, actual);
  }

  @Test
  public void checkOutputSchemaContainsInputSchemaDocs()
      throws ClassNotFoundException, SQLException, IOException {
    ResultSet rs = DbTestHelper.createConnection(CONNECTION_URL)
        .createStatement().executeQuery("SELECT * FROM COFFEES");
    String[] fieldNames = {"COF_NAME", "SUP_ID", "PRICE"};
    String[] fieldDocs = {
      "Input field COF_NAME doc", "Input field SUP_ID doc", "Input field PRICE doc"
    };

    final String recordName = "COFFEE";
    final String recordDoc = "Input record doc";
    final String recordNamespace = "Input record namespace";
    Schema inputSchema = createRecordSchema(recordName, recordDoc, recordNamespace, fieldNames,
        fieldDocs);

    Schema schema =
        JdbcAvroSchema.createAvroSchema(
            rs, "dbeam_generated", "connection", "doc", false, Optional.of(inputSchema));

    Assert.assertEquals("Input record doc", schema.getDoc());
    Assert.assertEquals("Input record namespace", schema.getNamespace());
    Assert.assertEquals("Input field COF_NAME doc", schema.getField("COF_NAME").doc());
    Assert.assertEquals("Input field SUP_ID doc", schema.getField("SUP_ID").doc());
    Assert.assertEquals("Input field PRICE doc", schema.getField("PRICE").doc());
  }

  private Schema createRecordSchema(
      final String recordName,
      final String recordDoc,
      final String recordNamespace,
      final String[] fieldNames,
      final String[] fieldDocs) {
    Schema inputSchema = Schema.createRecord(recordName, recordDoc, recordNamespace, false);
    final List<Schema.Field> fields = new ArrayList<>();
    for (int i = 0; i < fieldNames.length; i++) {
      String fieldName = fieldNames[i];
      String fieldDoc = fieldDocs[i];
      fields.add(new Schema.Field(fieldName, inputSchema,fieldDoc));
    }
    inputSchema.setFields(fields);
    
    return inputSchema;
  }
}
