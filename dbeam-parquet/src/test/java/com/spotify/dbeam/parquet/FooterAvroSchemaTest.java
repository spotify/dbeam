/*-
 * -\-\-
 * DBeam Parquet
 * --
 * Copyright (C) 2016 - 2025 Spotify AB
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

package com.spotify.dbeam.parquet;

import com.spotify.dbeam.DbTestHelper;
import com.spotify.dbeam.TestHelper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end checks that the {@code parquet.avro.schema} footer describes the written columns: the
 * output is read back through {@link AvroParquetReader} (as Scio/Beam ParquetIO do) and the footer
 * must equal {@code _AVRO_SCHEMA.avsc}.
 */
public class FooterAvroSchemaTest {

  private static final String CONNECTION_URL =
      "jdbc:h2:mem:footer_avro_schema;MODE=PostgreSQL;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1";
  private static final LocalDateTime TS = LocalDateTime.of(2026, 8, 20, 19, 6, 1, 879_123_000);
  private static final LocalDateTime DT = LocalDateTime.of(2026, 8, 20, 0, 0);
  private static Path testDir;
  private static Path passwordPath;

  @BeforeAll
  public static void beforeAll() throws Exception {
    testDir = TestHelper.createTmpDirPath("footer-avro-schema-test-");
    passwordPath = testDir.resolve(".password");
    passwordPath.toFile().createNewFile();
    try (Connection connection = DbTestHelper.createConnection(CONNECTION_URL)) {
      connection
          .createStatement()
          .execute(
              "CREATE TABLE ALL_KINDS (ID INT, BIG BIGINT, FLAG BOOLEAN, DBL DOUBLE PRECISION,"
                  + " RL REAL, TXT VARCHAR(20), BIN VARBINARY(8), DT DATE, TS TIMESTAMP(6),"
                  + " NUMS INTEGER ARRAY, TAGS VARCHAR(10) ARRAY)");
      // first row is all NULLs: the old JdbcAvroSchema footer failed on a NULL first ARRAY value
      connection
          .createStatement()
          .execute(
              "INSERT INTO ALL_KINDS VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,"
                  + " NULL, NULL)");
      connection
          .createStatement()
          .execute(
              "INSERT INTO ALL_KINDS VALUES (2, 20000000000, TRUE, 1.5, 2.5, 'txt', X'0102',"
                  + " DATE '2026-08-20', TIMESTAMP '2026-08-20 19:06:01.879123',"
                  + " ARRAY[1, NULL, 3], ARRAY['a', NULL])");
      connection.createStatement().execute("CREATE TABLE SIMPLE (ID INT, NAME VARCHAR(10))");
      connection.createStatement().execute("INSERT INTO SIMPLE VALUES (1, 'one'), (2, NULL)");
    }
  }

  private static Path runExport(final String name, final String table, final String... extra)
      throws Exception {
    final Path outputPath = testDir.resolve(name);
    final List<String> args =
        new ArrayList<>(
            List.of(
                "--targetParallelism=1",
                "--partition=2025-02-28",
                "--skipPartitionCheck",
                "--exportTimeout=PT1M",
                "--connectionUrl=" + CONNECTION_URL,
                "--username=",
                "--passwordFile=" + passwordPath,
                "--table=" + table,
                "--output=" + outputPath,
                "--avroSchemaNamespace=footer.test",
                "--avroDoc=footer doc"));
    args.addAll(Arrays.asList(extra));
    JdbcParquetJob.create(args.toArray(new String[0])).runExport();
    return outputPath;
  }

  /** Asserts footer == _AVRO_SCHEMA.avsc and returns the records read through the footer. */
  private static List<GenericRecord> readAndCheckFooter(final Path outputPath) throws IOException {
    final Path parquetFile = outputPath.resolve("part-00000-of-00001.parquet");
    final org.apache.hadoop.fs.Path hadoopPath =
        new org.apache.hadoop.fs.Path(parquetFile.toUri());
    final String footer;
    try (ParquetFileReader fileReader =
        ParquetFileReader.open(HadoopInputFile.fromPath(hadoopPath, new Configuration()))) {
      footer =
          fileReader
              .getFooter()
              .getFileMetaData()
              .getKeyValueMetaData()
              .get(JdbcParquetIO.PARQUET_AVRO_SCHEMA_KEY);
    }
    Assertions.assertNotNull(footer, "missing parquet.avro.schema footer");
    final Schema footerSchema = new Schema.Parser().parse(footer);
    final Schema avscSchema =
        new Schema.Parser()
            .parse(new String(Files.readAllBytes(outputPath.resolve("_AVRO_SCHEMA.avsc"))));
    Assertions.assertEquals(avscSchema, footerSchema);
    Assertions.assertEquals(avscSchema.toString(), footer);
    Assertions.assertEquals("footer.test", footerSchema.getNamespace());
    Assertions.assertEquals("footer doc", footerSchema.getDoc());

    final List<GenericRecord> records = new ArrayList<>();
    try (ParquetReader<GenericRecord> reader =
        AvroParquetReader.<GenericRecord>builder(
                HadoopInputFile.fromPath(hadoopPath, new Configuration()))
            .build()) {
      GenericRecord record;
      while ((record = reader.read()) != null) {
        Assertions.assertEquals(footerSchema, record.getSchema());
        records.add(record);
      }
    }
    return records;
  }

  private static Schema nonNull(final Schema schema) {
    Assertions.assertEquals(Schema.Type.UNION, schema.getType(), schema.toString());
    return schema.getTypes().get(1);
  }

  private static List<String> strings(final Object list) {
    return ((List<?>) list)
        .stream().map(o -> o == null ? null : o.toString()).collect(Collectors.toList());
  }

  private static void assertAllKinds(
      final List<GenericRecord> records, final long expectedTs, final long expectedDt) {
    Assertions.assertEquals(2, records.size());
    final GenericRecord nulls = records.get(0);
    for (Schema.Field field : nulls.getSchema().getFields()) {
      Assertions.assertNull(nulls.get(field.name()), field.name());
    }
    final GenericRecord values = records.get(1);
    Assertions.assertEquals(2, values.get("ID"));
    Assertions.assertEquals(20000000000L, values.get("BIG"));
    Assertions.assertEquals(true, values.get("FLAG"));
    Assertions.assertEquals(1.5, values.get("DBL"));
    Assertions.assertEquals(2.5f, values.get("RL"));
    Assertions.assertEquals("txt", values.get("TXT").toString());
    final java.nio.ByteBuffer bin = (java.nio.ByteBuffer) values.get("BIN");
    Assertions.assertEquals(java.nio.ByteBuffer.wrap(new byte[] {1, 2}), bin);
    Assertions.assertEquals(expectedDt, values.get("DT"));
    Assertions.assertEquals(expectedTs, values.get("TS"));
    Assertions.assertEquals(
        Arrays.asList(1, null, 3), new ArrayList<>((List<?>) values.get("NUMS")));
    Assertions.assertEquals(Arrays.asList("a", null), strings(values.get("TAGS")));
  }

  @Test
  public void shouldRoundTripAllKindsWithTimestampMicros() throws Exception {
    final Path out =
        runExport("allKindsMicros", "ALL_KINDS", "--useLogicalTypes", "--useTimestampMicros");
    final List<GenericRecord> records = readAndCheckFooter(out);
    final Schema schema = records.get(0).getSchema();
    Assertions.assertEquals(
        "timestamp-micros", nonNull(schema.getField("TS").schema()).getProp("logicalType"));
    Assertions.assertEquals(
        "timestamp-micros", nonNull(schema.getField("DT").schema()).getProp("logicalType"));
    final Schema nums = nonNull(schema.getField("NUMS").schema());
    Assertions.assertEquals(Schema.Type.ARRAY, nums.getType());
    // Parquet LIST elements are optional, so the Avro items must be nullable
    Assertions.assertEquals(
        Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT)),
        nums.getElementType());
    // field docs/props keep dbeam-core's JdbcAvroSchema conventions
    Assertions.assertEquals("TS", schema.getField("TS").getProp("columnName"));
    Assertions.assertTrue(schema.getField("TS").doc().startsWith("From sqlType 93 TIMESTAMP"));
    Assertions.assertEquals("ALL_KINDS", schema.getProp("tableName"));
    final Instant ts = TS.toInstant(ZoneOffset.UTC);
    assertAllKinds(
        records,
        ChronoUnit.MICROS.between(Instant.EPOCH, ts),
        DT.toInstant(ZoneOffset.UTC).toEpochMilli() * 1000);
  }

  @Test
  public void shouldRoundTripAllKindsWithTimestampMillis() throws Exception {
    final Path out = runExport("allKindsMillis", "ALL_KINDS", "--useLogicalTypes");
    final List<GenericRecord> records = readAndCheckFooter(out);
    Assertions.assertEquals(
        "timestamp-millis",
        nonNull(records.get(0).getSchema().getField("TS").schema()).getProp("logicalType"));
    assertAllKinds(
        records,
        TS.toInstant(ZoneOffset.UTC).toEpochMilli(),
        DT.toInstant(ZoneOffset.UTC).toEpochMilli());
  }

  @Test
  public void shouldRoundTripAllKindsWithoutLogicalTypes() throws Exception {
    final Path out = runExport("allKindsPlain", "ALL_KINDS");
    final List<GenericRecord> records = readAndCheckFooter(out);
    Assertions.assertNull(
        nonNull(records.get(0).getSchema().getField("TS").schema()).getProp("logicalType"));
    assertAllKinds(
        records,
        TS.toInstant(ZoneOffset.UTC).toEpochMilli(),
        DT.toInstant(ZoneOffset.UTC).toEpochMilli());
  }

  @Test
  public void shouldUseSchemaFileFieldNamesInFooter() throws Exception {
    final Path schemaFile = testDir.resolve("renamed.parquet.txt");
    Files.write(
        schemaFile,
        ("message renamed {\n"
                + "  optional int32 identifier;\n"
                + "  optional binary label (STRING);\n"
                + "}")
            .getBytes());
    final Path out =
        runExport("schemaFile", "SIMPLE", "--parquetSchemaFilePath=" + schemaFile);
    final List<GenericRecord> records = readAndCheckFooter(out);
    final Schema schema = records.get(0).getSchema();
    Assertions.assertEquals("renamed", schema.getName());
    Assertions.assertEquals(
        List.of("identifier", "label"),
        schema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList()));
    Assertions.assertEquals(
        Schema.Type.INT, nonNull(schema.getField("identifier").schema()).getType());
    // JDBC column info is matched by position
    Assertions.assertEquals("NAME", schema.getField("label").getProp("columnName"));
    Assertions.assertEquals(2, records.size());
    Assertions.assertEquals(1, records.get(0).get("identifier"));
    Assertions.assertEquals("one", records.get(0).get("label").toString());
    Assertions.assertEquals(2, records.get(1).get("identifier"));
    Assertions.assertNull(records.get(1).get("label"));
  }
}
