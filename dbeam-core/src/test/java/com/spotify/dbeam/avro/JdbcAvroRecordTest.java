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

import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.spotify.dbeam.Coffee;
import com.spotify.dbeam.DbTestHelper;
import com.spotify.dbeam.TestHelper;
import com.spotify.dbeam.args.QueryBuilderArgs;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class JdbcAvroRecordTest {

  private static String CONNECTION_URL =
      "jdbc:h2:mem:test;MODE=PostgreSQL;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1";

  public static GenericRecord readFirstGenericRecord(byte[] avroDocBytes, Schema schema)
      throws IOException {
    SeekableByteArrayInput inputStream = new SeekableByteArrayInput(avroDocBytes);
    DataFileReader<GenericRecord> dataReader = new DataFileReader<>(inputStream,
        new GenericDatumReader<>(schema));
    return dataReader.next();
  }

  @BeforeClass
  public static void beforeAll() throws SQLException, ClassNotFoundException {
    DbTestHelper.createFixtures(CONNECTION_URL);
  }

  @Test
  public void shouldCreateSchema() throws ClassNotFoundException, SQLException {
    final int fieldCount = 14;
    final Schema actual =
        JdbcAvroSchema.createSchemaByReadingOneRow(
            DbTestHelper.createConnection(CONNECTION_URL),
            QueryBuilderArgs.create("COFFEES"),
            "dbeam_generated",
            Optional.empty(),
            "Generate schema from JDBC ResultSet from COFFEES jdbc:h2:mem:test",
            false);

    Assert.assertNotNull(actual);
    Assert.assertEquals("dbeam_generated", actual.getNamespace());
    Assert.assertEquals("COFFEES", actual.getProp("tableName"));
    Assert.assertEquals("jdbc:h2:mem:test", actual.getProp("connectionUrl"));
    Assert.assertEquals(
        "Generate schema from JDBC ResultSet from COFFEES jdbc:h2:mem:test", actual.getDoc());
    Assert.assertEquals(fieldCount, actual.getFields().size());
    Assert.assertEquals(
        Lists.newArrayList(
            "COF_NAME",
            "SUP_ID",
            "PRICE",
            "TEMPERATURE",
            "SIZE",
            "IS_ARABIC",
            "SALES",
            "TOTAL",
            "CREATED",
            "UPDATED",
            "UID",
            "ROWNUM",
            "INT_ARR",
            "TEXT_ARR"),
        actual.getFields().stream().map(Schema.Field::name).collect(Collectors.toList()));
    for (Schema.Field f : actual.getFields()) {
      Assert.assertEquals(Schema.Type.UNION, f.schema().getType());
      Assert.assertEquals(2, f.schema().getTypes().size());
      Assert.assertEquals(Schema.Type.NULL, f.schema().getTypes().get(0).getType());
    }
    Assert.assertEquals(
        Schema.Type.STRING, actual.getField("COF_NAME").schema().getTypes().get(1).getType());
    Assert.assertEquals(
        Schema.Type.INT, actual.getField("SUP_ID").schema().getTypes().get(1).getType());
    Assert.assertEquals(
        Schema.Type.STRING, actual.getField("PRICE").schema().getTypes().get(1).getType());
    Assert.assertEquals(
        Schema.Type.FLOAT, actual.getField("TEMPERATURE").schema().getTypes().get(1).getType());
    Assert.assertEquals(
        Schema.Type.DOUBLE, actual.getField("SIZE").schema().getTypes().get(1).getType());
    Assert.assertEquals(
        Schema.Type.BOOLEAN, actual.getField("IS_ARABIC").schema().getTypes().get(1).getType());
    Assert.assertEquals(
        Schema.Type.INT, actual.getField("SALES").schema().getTypes().get(1).getType());
    Assert.assertEquals(
        Schema.Type.LONG, actual.getField("TOTAL").schema().getTypes().get(1).getType());
    Assert.assertEquals(
        Schema.Type.LONG, actual.getField("CREATED").schema().getTypes().get(1).getType());
    Assert.assertEquals(
        Schema.Type.LONG, actual.getField("UPDATED").schema().getTypes().get(1).getType());
    Assert.assertEquals(
        Schema.Type.BYTES, actual.getField("UID").schema().getTypes().get(1).getType());
    Assert.assertEquals(
        Schema.Type.LONG, actual.getField("ROWNUM").schema().getTypes().get(1).getType());
    Assert.assertNull(actual.getField("UPDATED").schema().getTypes().get(1).getProp("logicalType"));
  }

  @Test
  public void shouldCreateSchemaWithLogicalTypes() throws ClassNotFoundException, SQLException {
    final int fieldCount = 14;
    final Schema actual =
        JdbcAvroSchema.createSchemaByReadingOneRow(
            DbTestHelper.createConnection(CONNECTION_URL),
            QueryBuilderArgs.create("COFFEES"),
            "dbeam_generated",
            Optional.empty(),
            "Generate schema from JDBC ResultSet from COFFEES jdbc:h2:mem:test",
            true);

    Assert.assertEquals(fieldCount, actual.getFields().size());
    Assert.assertEquals(
        "timestamp-millis",
        actual.getField("UPDATED").schema().getTypes().get(1).getProp("logicalType"));
  }

  @Test
  public void shouldCreateSchemaWithCustomSchemaName() throws ClassNotFoundException, SQLException {
    final Schema actual =
        JdbcAvroSchema.createSchemaByReadingOneRow(
            DbTestHelper.createConnection(CONNECTION_URL),
            QueryBuilderArgs.create("COFFEES"),
            "dbeam_generated",
            Optional.of("CustomSchemaName"),
            "Generate schema from JDBC ResultSet from COFFEES jdbc:h2:mem:test",
            false);

    Assert.assertEquals("CustomSchemaName", actual.getName());
  }

  @Test
  public void shouldEncodeUUIDValue() throws SQLException, IOException {
    final ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getColumnType(1)).thenReturn(Types.OTHER);
    when(meta.getColumnName(1)).thenReturn("test1");
    when(meta.getColumnClassName(1)).thenReturn("java.util.UUID");
    when(meta.getColumnTypeName(1)).thenReturn("uuid");

    final ResultSet resultSet = Mockito.mock(ResultSet.class);
    when(resultSet.getMetaData()).thenReturn(meta);
    final UUID expected = UUID.randomUUID();
    when(resultSet.getObject(1)).thenReturn(expected);
    final Schema schema = JdbcAvroSchema.createAvroSchema(resultSet, "ns", "conn_url",
        Optional.empty(), "doc", true);
    final JdbcAvroRecordConverter converter = JdbcAvroRecordConverter.create(resultSet);

    DataFileWriter<GenericRecord> dataFileWriter =
        new DataFileWriter<>(new GenericDatumWriter<>(schema));
    ByteArrayOutputStream avroOutputStream = new ByteArrayOutputStream();
    dataFileWriter.create(schema, avroOutputStream);
    dataFileWriter.appendEncoded(converter.convertResultSetIntoAvroBytes());
    dataFileWriter.close();

    final GenericRecord rec = readFirstGenericRecord(avroOutputStream.toByteArray(), schema);
    Assert.assertEquals(rec.get("test1"), new Utf8(expected.toString()));
  }

  @Test
  public void shouldEncodeResultSetToValidAvro()
      throws ClassNotFoundException, SQLException, IOException {
    final ResultSet rs =
        DbTestHelper.createConnection(CONNECTION_URL)
            .createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY)
            .executeQuery("SELECT * FROM COFFEES");

    rs.first();
    final Schema schema =
        JdbcAvroSchema.createAvroSchema(
            rs, "dbeam_generated", "connection", Optional.empty(), "doc", false);
    final JdbcAvroRecordConverter converter = JdbcAvroRecordConverter.create(rs);
    final DataFileWriter<GenericRecord> dataFileWriter =
        new DataFileWriter<>(new GenericDatumWriter<>(schema));
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    dataFileWriter.create(schema, outputStream);
    rs.previous();
    // convert and write
    while (rs.next()) {
      dataFileWriter.appendEncoded(converter.convertResultSetIntoAvroBytes());
    }
    dataFileWriter.flush();
    outputStream.close();
    // transform to generic record
    final SeekableByteArrayInput inputStream =
        new SeekableByteArrayInput(outputStream.toByteArray());
    final DataFileReader<GenericRecord> dataFileReader =
        new DataFileReader<>(inputStream, new GenericDatumReader<>(schema));
    final List<GenericRecord> records =
        StreamSupport.stream(dataFileReader.spliterator(), false).collect(Collectors.toList());

    Assert.assertEquals(2, records.size());
    final GenericRecord record =
        records.stream()
            .filter(r -> Coffee.COFFEE1.name().equals(r.get(0).toString()))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("not found"));

    Assert.assertEquals(14, record.getSchema().getFields().size());
    Assert.assertEquals(schema, record.getSchema());
    List<String> actualTxtArray =
        ((GenericData.Array<Utf8>) record.get(13))
            .stream().map(x -> x.toString()).collect(Collectors.toList());
    final Coffee actual =
        Coffee.create(
            record.get(0).toString(),
            Optional.ofNullable((Integer) record.get(1)),
            new java.math.BigDecimal(record.get(2).toString()),
            (Float) record.get(3),
            (Double) record.get(4),
            (Boolean) record.get(5),
            (Integer) record.get(6),
            (Long) record.get(7),
            new java.sql.Timestamp((Long) record.get(8)),
            Optional.ofNullable((Long) record.get(9)).map(Timestamp::new),
            TestHelper.byteBufferToUuid((ByteBuffer) record.get(10)),
            (Long) record.get(11),
            new ArrayList<>((GenericData.Array<Integer>) record.get(12)),
            actualTxtArray);
    Assert.assertEquals(Coffee.COFFEE1, actual);
  }

  @Test
  public void shouldCorrectlyEncodeUnsignedIntToAvroLong() throws SQLException {
    // Note: UNSIGNED is not part of SQL standard and not supported by H2 and PSQL
    // https://github.com/h2database/h2database/issues/739
    // Still, testing here via mocks since MySQL has support for it
    final long valueUnderTest = 2190526558L; // MySQL Type INT Maximum Value Signed = 2147483647L
    final int columnNum = 1;

    final ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
    when(metadata.getColumnType(columnNum)).thenReturn(java.sql.Types.INTEGER);
    when(metadata.getColumnClassName(columnNum)).thenReturn("java.lang.Long");

    final JdbcAvroRecord.SqlFunction<ResultSet, Object> mapping =
        JdbcAvroRecord.computeMapping(metadata, columnNum);

    final ResultSet resultSet = Mockito.mock(ResultSet.class);
    when(resultSet.getLong(columnNum)).thenReturn(valueUnderTest);
    final Object result = mapping.apply(resultSet);

    Assert.assertEquals(Long.class, result.getClass());
    Assert.assertEquals(valueUnderTest, ((Long) result).longValue());
  }
}
