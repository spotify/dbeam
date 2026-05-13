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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BeamJdbcParquetSchemaTest {

  @Test
  public void shouldParseInputSchemaFile() throws IOException {
    final Path schemaFile = Files.createTempFile("test-schema-", ".parquet.txt");
    Files.write(
        schemaFile,
        ("message test_table {\n"
                + "  optional int64 id;\n"
                + "  optional binary name (STRING);\n"
                + "}")
            .getBytes());

    final Optional<MessageType> schema =
        BeamJdbcParquetSchema.parseOptionalInputParquetSchemaFile(schemaFile.toString());

    Assertions.assertTrue(schema.isPresent());
    Assertions.assertEquals("test_table", schema.get().getName());
    Assertions.assertEquals(2, schema.get().getFieldCount());

    Files.deleteIfExists(schemaFile);
  }

  @Test
  public void shouldReturnEmptyForNullFilename() throws IOException {
    final Optional<MessageType> schema =
        BeamJdbcParquetSchema.parseOptionalInputParquetSchemaFile(null);

    Assertions.assertFalse(schema.isPresent());
  }

  @Test
  public void shouldReturnEmptyForEmptyFilename() throws IOException {
    final Optional<MessageType> schema =
        BeamJdbcParquetSchema.parseOptionalInputParquetSchemaFile("");

    Assertions.assertFalse(schema.isPresent());
  }
}
