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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

/**
 * Verifies a dbeam-parquet export of e2e/ddl.sql demo_table by reading every output file through
 * its footer Avro schema, as Scio/Beam ParquetIO consumers do.
 *
 * <p>Usage: {@code VerifyParquetOutput <outputDir> <expectedRowsPerRun> [--logicalTypes]
 * [--micros] [--arrayMode=bytes] [--dataOnly]}. Every directory under {@code outputDir} holding
 * {@code .parquet} files is one export (e.g. {@code run_0}, {@code run_1} of BenchJdbcParquetJob).
 */
public final class VerifyParquetOutput {

  // demo_table timestamps are between 2000-01-01 and 2010-01-01
  private static final long MIN_MILLIS = 946684800000L;
  private static final long MAX_MILLIS = 1262304000000L;

  private VerifyParquetOutput() {}

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      throw new IllegalArgumentException(
          "Usage: VerifyParquetOutput <outputDir> <expectedRowsPerRun> [--logicalTypes] [--micros]"
              + " [--arrayMode=bytes] [--dataOnly]");
    }
    final java.nio.file.Path outputDir = Paths.get(args[0]);
    final long expectedRows = Long.parseLong(args[1]);
    final List<String> flags = List.of(args).subList(2, args.length);
    final boolean logicalTypes = flags.contains("--logicalTypes");
    final boolean micros = flags.contains("--micros");
    final boolean bytesArrays = flags.contains("--arrayMode=bytes");
    final boolean dataOnly = flags.contains("--dataOnly");

    final Map<java.nio.file.Path, List<java.nio.file.Path>> filesByRun = new TreeMap<>();
    try (Stream<java.nio.file.Path> paths = Files.walk(outputDir)) {
      for (java.nio.file.Path p : paths.collect(Collectors.toList())) {
        if (p.getFileName().toString().endsWith(".parquet")) {
          filesByRun.computeIfAbsent(p.getParent(), k -> new ArrayList<>()).add(p);
        }
      }
    }
    if (filesByRun.isEmpty()) {
      throw new AssertionError("No .parquet files under " + outputDir);
    }
    for (Map.Entry<java.nio.file.Path, List<java.nio.file.Path>> run : filesByRun.entrySet()) {
      final java.nio.file.Path runDir = run.getKey();
      final java.nio.file.Path avsc = runDir.resolve("_AVRO_SCHEMA.avsc");
      if (dataOnly) {
        for (String name : List.of("_AVRO_SCHEMA.avsc", "_PARQUET_SCHEMA.json", "_queries")) {
          check(!Files.exists(runDir.resolve(name)), runDir + "/" + name + " with --dataOnly");
        }
      } else {
        check(Files.exists(avsc), "missing " + avsc);
      }
      final RunStats stats = new RunStats();
      for (java.nio.file.Path file : run.getValue()) {
        verifyFooter(file, dataOnly ? null : avsc);
        readFile(file, logicalTypes, micros, bytesArrays, stats);
      }
      check(
          stats.rows == expectedRows,
          String.format("%s: expected %d rows, got %d", runDir, expectedRows, stats.rows));
      // row_number is 1..N, each exactly once
      check(
          stats.minRowNumber == 1
              && stats.maxRowNumber == expectedRows
              && stats.rowNumberSum == expectedRows * (expectedRows + 1) / 2,
          String.format(
              "%s: row_number not 1..%d (min %d, max %d, sum %d)",
              runDir, expectedRows, stats.minRowNumber, stats.maxRowNumber, stats.rowNumberSum));
      System.out.printf(
          "OK: %s: %d files, %d rows%n", runDir, run.getValue().size(), stats.rows);
    }
  }

  private static final class RunStats {
    long rows = 0;
    long rowNumberSum = 0;
    long minRowNumber = Long.MAX_VALUE;
    long maxRowNumber = Long.MIN_VALUE;
  }

  private static void verifyFooter(final java.nio.file.Path file, final java.nio.file.Path avsc)
      throws IOException {
    final String footer;
    try (ParquetFileReader reader =
        ParquetFileReader.open(
            HadoopInputFile.fromPath(new Path(file.toUri()), new Configuration()))) {
      footer = reader.getFooter().getFileMetaData().getKeyValueMetaData().get("parquet.avro.schema");
    }
    check(footer != null, file + ": parquet.avro.schema footer key missing");
    final Schema footerSchema = new Schema.Parser().parse(footer);
    if (avsc != null) {
      final Schema expected = new Schema.Parser().parse(avsc.toFile());
      check(expected.equals(footerSchema), file + ": footer schema differs from " + avsc);
    }
  }

  private static Schema nonNull(final Schema schema) {
    if (schema.getType() == Schema.Type.UNION) {
      for (Schema branch : schema.getTypes()) {
        if (branch.getType() != Schema.Type.NULL) {
          return branch;
        }
      }
    }
    return schema;
  }

  private static void readFile(
      final java.nio.file.Path file,
      final boolean logicalTypes,
      final boolean micros,
      final boolean bytesArrays,
      final RunStats stats)
      throws IOException {
    try (ParquetReader<GenericRecord> reader =
        AvroParquetReader.<GenericRecord>builder(
                HadoopInputFile.fromPath(new Path(file.toUri()), new Configuration()))
            .build()) {
      GenericRecord record;
      boolean first = true;
      while ((record = reader.read()) != null) {
        if (first) {
          verifySchema(file, record.getSchema(), logicalTypes, micros);
          first = false;
        }
        verifyRecord(file, record, micros, bytesArrays);
        final long rowNumber = (Long) record.get("row_number");
        stats.rows++;
        stats.rowNumberSum += rowNumber;
        stats.minRowNumber = Math.min(stats.minRowNumber, rowNumber);
        stats.maxRowNumber = Math.max(stats.maxRowNumber, rowNumber);
      }
    }
  }

  private static void verifySchema(
      final java.nio.file.Path file,
      final Schema schema,
      final boolean logicalTypes,
      final boolean micros) {
    final String expectedTimestamp =
        logicalTypes ? (micros ? "timestamp-micros" : "timestamp-millis") : null;
    for (String field : List.of("timestamp1", "timestamp2")) {
      final String actual = nonNull(schema.getField(field).schema()).getProp("logicalType");
      check(
          java.util.Objects.equals(expectedTimestamp, actual),
          String.format("%s: %s logicalType %s, expected %s", file, field, actual,
              expectedTimestamp));
    }
    final String uuidType = nonNull(schema.getField("uuid1").schema()).getProp("logicalType");
    check(
        java.util.Objects.equals(logicalTypes ? "uuid" : null, uuidType),
        file + ": uuid1 logicalType " + uuidType);
  }

  private static void verifyRecord(
      final java.nio.file.Path file,
      final GenericRecord record,
      final boolean micros,
      final boolean bytesArrays) {
    final String where = file + " row_number=" + record.get("row_number");
    check("const".equals(String.valueOf(record.get("flag1"))), where + ": flag1");
    check("const".equals(String.valueOf(record.get("flag2"))), where + ": flag2");
    check(String.valueOf(record.get("hexid1")).length() == 32, where + ": hexid1");
    check(String.valueOf(record.get("random_str2")).length() == 12, where + ": random_str2");
    final int tag = (Integer) record.get("tag_field_id");
    check(tag >= 1 && tag <= 10, where + ": tag_field_id " + tag);
    UUID.fromString(String.valueOf(record.get("uuid1")));
    for (String field : List.of("timestamp1", "timestamp2")) {
      final long value = (Long) record.get(field);
      final long millis = micros ? value / 1000 : value;
      check(
          millis >= MIN_MILLIS && millis <= MAX_MILLIS,
          where + ": " + field + "=" + value + " outside 2000..2010 in the expected unit");
    }
    final Object numeric = record.get("numeric_field");
    if (numeric != null) {
      check(
          List.of("1.99", "5.99", "99.99", "155.98").contains(numeric.toString()),
          where + ": numeric_field " + numeric);
    }
    check(record.get("bytes_field") instanceof ByteBuffer, where + ": bytes_field");
    if (bytesArrays) {
      for (String field : List.of("arr1", "arr2", "arr3", "arr5", "arr6")) {
        check(record.get(field) instanceof ByteBuffer, where + ": " + field + " not bytes");
      }
      return;
    }
    check(List.of("foo", "bar").equals(strings(record.get("arr1"))), where + ": arr1");
    check(List.of("foo", "bar").equals(strings(record.get("arr5"))), where + ": arr5");
    check(List.of(42, 777).equals(record.get("arr2")), where + ": arr2 " + record.get("arr2"));
    check(
        List.of(21474836471L, 21474836479L).equals(record.get("arr3")),
        where + ": arr3 " + record.get("arr3"));
    final List<String> uuids = strings(record.get("arr6"));
    check(uuids.size() == 2, where + ": arr6");
    uuids.forEach(UUID::fromString);
  }

  private static List<String> strings(final Object list) {
    return ((List<?>) list).stream().map(String::valueOf).collect(Collectors.toList());
  }

  private static void check(final boolean condition, final String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }
}
