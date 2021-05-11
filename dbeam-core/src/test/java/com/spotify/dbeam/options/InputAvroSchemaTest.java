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

package com.spotify.dbeam.options;

import com.spotify.dbeam.args.QueryBuilderArgs;
import com.spotify.dbeam.args.QueryBuilderArgsTest;
import com.spotify.dbeam.avro.BeamJdbcAvroSchema;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class InputAvroSchemaTest {

  private static File avroSchemaFile;
  private static Path avroSchemaFilePath;
  private static String avroSchemaFilePathStr;

  @BeforeClass
  public static void beforeAll() throws IOException {
    final String jsonSchema =
        "{\n"
            + "  \"name\": \"Show\",\n"
            + "  \"doc\": \"Record description\",\n"
            + "  \"namespace\": \"v2\",\n"
            + "  \"type\": \"record\",\n"
            + "  \"fields\": [\n"
            + "    {\n"
            + "      \"name\": \"field1\",\n"
            + "      \"type\": \"string\",\n"
            + "      \"doc\": \"Field1 description\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"field2\",\n"
            + "      \"type\": \"string\",\n"
            + "      \"doc\": \"Field2 description\"\n"
            + "    }\n"
            + "  ]\n"
            + "}\n";

    avroSchemaFile = createTestAvroSchemaFile(jsonSchema);
    avroSchemaFilePath = avroSchemaFile.toPath();
    avroSchemaFilePathStr = avroSchemaFilePath.toString();
  }

  private static File createTestAvroSchemaFile(final String jsonSchema) throws IOException {
    final File newTempFile = File.createTempFile("dataType1", ".avsc");
    newTempFile.deleteOnExit();
    Files.write(
        newTempFile.toPath(),
        jsonSchema.getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.CREATE);
    return newTempFile;
  }

  private Schema createRecordSchema(
      final String recordName,
      final String recordDoc,
      final String recordNamespace,
      final String[] fieldNames,
      final String[] fieldDocs) {
    final Schema inputSchema = Schema.createRecord(recordName, recordDoc, recordNamespace, false);
    final List<Schema.Field> fields = new ArrayList<>();
    for (int i = 0; i < fieldNames.length; i++) {
      String fieldName = fieldNames[i];
      String fieldDoc = fieldDocs[i];
      fields.add(new Schema.Field(fieldName, inputSchema, fieldDoc));
    }
    inputSchema.setFields(fields);

    return inputSchema;
  }

  @AfterClass
  public static void afterAll() throws IOException {
    Files.delete(avroSchemaFile.toPath());
  }

  @Test
  public void checkReadAvroSchema() throws IOException {
    final JdbcExportPipelineOptions options =
        PipelineOptionsFactory.create().as(JdbcExportPipelineOptions.class);
    options.setAvroSchemaFilePath(avroSchemaFilePathStr);

    Assert.assertEquals(avroSchemaFilePathStr, options.getAvroSchemaFilePath());

    final Schema inputSchema = BeamJdbcAvroSchema.parseInputAvroSchemaFile(avroSchemaFilePathStr);

    Assert.assertEquals("Record description", inputSchema.getDoc());
    Assert.assertEquals("Field1 description", inputSchema.getField("field1").doc());
    Assert.assertEquals("Field2 description", inputSchema.getField("field2").doc());
  }

  @Test
  @Ignore
  public void checkFullPath() {
    // TODO
    // Check provide input string to args and verify final schema

    final String[] fieldNames = null;
    final String[] fieldDocs = null;
    final String recordName = "COFFEE";
    final String recordDoc = "Input record doc";
    final String recordNamespace = "Input record namespace";
    final Schema inputSchema =
        createRecordSchema(recordName, recordDoc, recordNamespace, fieldNames, fieldDocs);
  }

  @Test
  public void checkReadAvroSchemaWithEmptyParameter() throws IOException {
    final String path = "";
    final JdbcExportPipelineOptions options =
        PipelineOptionsFactory.create().as(JdbcExportPipelineOptions.class);
    options.setAvroSchemaFilePath(path);

    Assert.assertEquals(path, options.getAvroSchemaFilePath());
    Assert.assertEquals(
        Optional.empty(), BeamJdbcAvroSchema.parseOptionalInputAvroSchemaFile(path));
  }

  @Test
  public void checkReadAvroSchemaWithNullParameter() throws IOException {
    final String path = null;
    final JdbcExportPipelineOptions options =
        PipelineOptionsFactory.create().as(JdbcExportPipelineOptions.class);
    options.setAvroSchemaFilePath(path);

    Assert.assertEquals(path, options.getAvroSchemaFilePath());
    Assert.assertEquals(
        Optional.empty(), BeamJdbcAvroSchema.parseOptionalInputAvroSchemaFile(path));
  }

  @Test(expected = SchemaParseException.class)
  public void checkReadAvroSchemaWithInvalidFormat() throws IOException {
    final String invalidJson = "{";
    final File invalidFile = createTestAvroSchemaFile(invalidJson);
    final String path = invalidFile.toPath().toString();
    final JdbcExportPipelineOptions options =
        PipelineOptionsFactory.create().as(JdbcExportPipelineOptions.class);
    options.setAvroSchemaFilePath(path);

    Assert.assertEquals(path, options.getAvroSchemaFilePath());
    BeamJdbcAvroSchema.parseOptionalInputAvroSchemaFile(path);
  }

  @Test(expected = FileNotFoundException.class)
  public void checkReadAvroSchemaWithNonExistentFile() throws IOException {
    final String path = "non_existent_schema.avsc";
    final JdbcExportPipelineOptions options =
        PipelineOptionsFactory.create().as(JdbcExportPipelineOptions.class);
    options.setAvroSchemaFilePath(path);

    Assert.assertEquals(path, options.getAvroSchemaFilePath());
    BeamJdbcAvroSchema.parseOptionalInputAvroSchemaFile(path);
  }

  @Test
  public void checkValidCommandLineArgIsParsedAsOptions() throws IOException, SQLException {
    final JdbcExportPipelineOptions options =
        QueryBuilderArgsTest.commandLineToOptions(
            "--connectionUrl=jdbc:postgresql://some_db --table=some_table "
                + "--avroSchemaFilePath=/temp/record1.avsc --partition=2027-07-31");

    Assert.assertEquals("/temp/record1.avsc", options.getAvroSchemaFilePath());
  }

  @Test
  public void checkEmptyCommandLineArgIsParsedAsOptions() throws IOException, SQLException {
    final JdbcExportPipelineOptions options =
        QueryBuilderArgsTest.commandLineToOptions(
            "--connectionUrl=jdbc:postgresql://some_db --table=some_table "
                + "--partition=2027-07-31");

    Assert.assertNull(options.getAvroSchemaFilePath());
  }

  private QueryBuilderArgs pareOptions(String cmdLineArgs) throws IOException {
    PipelineOptionsFactory.register(JdbcExportPipelineOptions.class);
    final JdbcExportPipelineOptions opts =
        PipelineOptionsFactory.fromArgs(cmdLineArgs.split(" "))
            .withValidation()
            .create()
            .as(JdbcExportPipelineOptions.class);
    return JdbcExportArgsFactory.createQueryArgs(opts);
  }
}
