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

package com.spotify.dbeam.jobs;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.spotify.dbeam.JavaSqlHelper;
import com.spotify.dbeam.args.JdbcAvroArgs;
import com.spotify.dbeam.args.JdbcExportArgs;
import com.spotify.dbeam.args.QueryBuilderArgs;
import com.spotify.dbeam.args.QueryBuilderArgsTest;
import com.spotify.dbeam.avro.BeamJdbcAvroSchema;
import com.spotify.dbeam.options.JdbcExportPipelineOptions;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

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

  @AfterClass
  public static void afterAll() throws IOException {
    Files.delete(avroSchemaFile.toPath());
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
  public void checkSuppliedAvroSchemaValidationPassed() throws Exception {

    int columnCountInResultSet = 2;
    // Since our supplied schema has 2 fields, validation should fail.

    Schema schema = createAndVerifySuppliedSchema(columnCountInResultSet);

    Assert.assertEquals(2, schema.getFields().size());
  }

  private Schema createAndVerifySuppliedSchema(int columnCount) throws Exception {
    final JdbcExportPipelineOptions options =
        PipelineOptionsFactory.create().as(JdbcExportPipelineOptions.class);
    options.setAvroSchemaFilePath(avroSchemaFilePathStr);

    PipelineOptions pipelineOptions = options;
    final Pipeline pipeline = TestPipeline.create();

    final JdbcExportArgs jdbcExportArgs =
        JdbcExportArgs.create(
            // JdbcAvroArgs.create(JdbcConnectionArgs.create("dummyUrl")),
            Mockito.mock(JdbcAvroArgs.class),
            QueryBuilderArgs.create("dummyTable"),
            "avroSchemaNamespace",
            Optional.empty(),
            Optional.empty(),
            false,
            Duration.ofSeconds(30));

    // mocks set-up
    DatabaseMetaData meta = Mockito.mock(DatabaseMetaData.class);
    when(meta.getURL()).thenReturn("dummyUrl");

    ResultSetMetaData rsMetaData = JavaSqlHelper.DummyResultSetMetaData.create(columnCount);

    ResultSet resultSet = Mockito.mock(ResultSet.class);
    when(resultSet.getMetaData()).thenReturn(rsMetaData);

    Statement statement = Mockito.mock(Statement.class);
    when(statement.executeQuery(anyString())).thenReturn(resultSet);

    Connection connection = Mockito.mock(Connection.class);
    when(connection.getMetaData()).thenReturn(meta);

    when(connection.createStatement()).thenReturn(statement);

    final String output = "output";
    final boolean dataOnly = true;
    final long minRows = -1L;

    final JdbcAvroJob job =
        new JdbcAvroJob(pipelineOptions, pipeline, jdbcExportArgs, output, dataOnly, minRows);

    return job.createSchema(connection);
  }

  @Test
  public void checkReadAvroSchemaWithEmptyParameter() throws IOException {
    final String path = "";
    final JdbcExportPipelineOptions options =
        PipelineOptionsFactory.create().as(JdbcExportPipelineOptions.class);
    options.setAvroSchemaFilePath(path);

    Assert.assertEquals(path, options.getAvroSchemaFilePath());
  }

  @Test
  public void checkReadAvroSchemaWithNullParameter() throws IOException {
    final String path = null;
    final JdbcExportPipelineOptions options =
        PipelineOptionsFactory.create().as(JdbcExportPipelineOptions.class);
    options.setAvroSchemaFilePath(path);

    Assert.assertEquals(path, options.getAvroSchemaFilePath());
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
    BeamJdbcAvroSchema.parseInputAvroSchemaFile(path);
  }

  @Test(expected = FileNotFoundException.class)
  public void checkReadAvroSchemaWithNonExistentFile() throws IOException {
    final String path = "non_existent_schema.avsc";
    final JdbcExportPipelineOptions options =
        PipelineOptionsFactory.create().as(JdbcExportPipelineOptions.class);
    options.setAvroSchemaFilePath(path);

    Assert.assertEquals(path, options.getAvroSchemaFilePath());
    BeamJdbcAvroSchema.parseInputAvroSchemaFile(path);
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
}
