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

package com.spotify.dbeam.args;

import com.spotify.dbeam.jobs.JdbcAvroJob;
import com.spotify.dbeam.options.JdbcExportArgsFactory;
import com.spotify.dbeam.options.JdbcExportPipelineOptions;
import com.spotify.dbeam.options.OutputOptions;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Period;
import java.util.Optional;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class JdbcExportOptionsTest {
  private static File sqlFile;

  @BeforeAll
  public static void beforeAll() throws IOException {
    sqlFile = File.createTempFile("query", ".sql");
    sqlFile.deleteOnExit();
  }

  @AfterAll
  public static void afterAll() throws IOException {
    Files.delete(sqlFile.toPath());
  }

  JdbcExportArgs optionsFromArgs(String cmdLineArgs) throws IOException, ClassNotFoundException {
    return optionsFromArgs(cmdLineArgs.split(" "));
  }

  JdbcExportArgs optionsFromArgs(String[] cmdLineArgs) throws IOException, ClassNotFoundException {
    PipelineOptionsFactory.register(JdbcExportPipelineOptions.class);
    final PipelineOptions opts =
        PipelineOptionsFactory.fromArgs(cmdLineArgs).withValidation().create();
    return JdbcExportArgsFactory.fromPipelineOptions(opts);
  }

  @Test
  public void shouldFailParseOnInvalidArg() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> optionsFromArgs("--foo=bar"));
  }

  @Test
  public void shouldFailOnMissingConnectionUrl() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> optionsFromArgs("--table=sometable"));
  }

  @Test
  public void shouldFailOnMissingTableAndSqlFile() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db"));
  }

  @Test
  public void shouldFailOnTableAndSqlFilePresent() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            optionsFromArgs(
                "--connectionUrl=jdbc:postgresql://some_db --sqlFile="
                    + sqlFile.getAbsolutePath()
                    + " --table=some_table"));
  }

  @Test
  public void shouldNotFailOnMissingTableSqlFile() throws IOException, ClassNotFoundException {
    JdbcExportArgs actual =
        optionsFromArgs(
            "--connectionUrl=jdbc:postgresql://some_db --sqlFile=" + sqlFile.getAbsolutePath());

    final JdbcExportArgs expected =
        JdbcExportArgs.create(
            JdbcAvroArgs.create(
                JdbcConnectionArgs.create("jdbc:postgresql://some_db")
                    .withUsername("dbeam-extractor")),
            QueryBuilderArgs.createFromQuery(
                com.google.common.io.Files.asCharSource(sqlFile, StandardCharsets.UTF_8).read()));

    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void shouldParseWithDefaultsOnConnectionUrlAndTable()
      throws IOException, ClassNotFoundException {
    final JdbcExportArgs actual =
        optionsFromArgs("--connectionUrl=jdbc:postgresql://some_db --table=some_table");

    final JdbcExportArgs expected =
        JdbcExportArgs.create(
            JdbcAvroArgs.create(
                JdbcConnectionArgs.create("jdbc:postgresql://some_db")
                    .withUsername("dbeam-extractor")),
            QueryBuilderArgs.create("some_table"));

    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void shouldParseWithMySqlConnection() throws IOException, ClassNotFoundException {
    final JdbcExportArgs actual =
        optionsFromArgs("--connectionUrl=jdbc:mysql://some_db --table=some_table");

    final JdbcExportArgs expected =
        JdbcExportArgs.create(
            JdbcAvroArgs.create(
                JdbcConnectionArgs.create("jdbc:mysql://some_db").withUsername("dbeam-extractor")),
            QueryBuilderArgs.create("some_table"));

    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void shouldFailOnInvalidTable() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            optionsFromArgs(
                "--connectionUrl=jdbc:postgresql://some_db --table=some-table-with-dash"));
  }

  @Test
  public void shouldFailOnNonJdbcUrl() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> optionsFromArgs("--connectionUrl=bar --table=sometable"));
  }

  @Test
  public void shouldFailOnUnsupportedJdbcUrl() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> optionsFromArgs("--connectionUrl=jdbc:paradox:./foo --table=sometable"));
  }

  @Test
  public void shouldFailOnMissingPartitionButPresentPartitionColumn() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            optionsFromArgs(
                "--connectionUrl=jdbc:postgresql://some_db --table=sometable "
                    + "--partitionColumn=col"));
  }

  @Test
  public void shouldFailOnTooOldPartition() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            optionsFromArgs(
                "--connectionUrl=jdbc:postgresql://some_db --table=sometable "
                    + "--partition=2015-01-01"));
  }

  @Test
  public void shouldFailOnTooOldPartitionWithConfiguredMinPartitionPeriodMoreThanPartition() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            optionsFromArgs(
                "--connectionUrl=jdbc:postgresql://some_db --table=sometable "
                    + "--partition=2015-01-01 --minPartitionPeriod=2015-01-02"));
  }

  @Test
  public void shouldFailOnTooOldPartitionWithConfiguredMinPartitionPeriodLessThanPartition() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            optionsFromArgs(
                "--connectionUrl=jdbc:postgresql://some_db --table=sometable "
                    + "--partition=2015-01-01 --minPartitionPeriod=2015-01-01"));
  }

  @Test
  public void shouldFailOnNonJdbcUrl2() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> optionsFromArgs("--connectionUrl=some:foo:bar --table=sometable"));
  }

  @Test
  public void shouldConfigureUserAndPassword() throws IOException, ClassNotFoundException {
    final JdbcExportArgs actual =
        optionsFromArgs(
            "--connectionUrl=jdbc:postgresql://some_db --table=some_table "
                + "--username=someuser --password=somepassword");

    final JdbcExportArgs expected =
        JdbcExportArgs.create(
            JdbcAvroArgs.create(
                JdbcConnectionArgs.create("jdbc:postgresql://some_db")
                    .withUsername("someuser")
                    .withPassword("somepassword")),
            QueryBuilderArgs.create("some_table"));

    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void shouldConfigureAvroLogicalTypes() throws IOException, ClassNotFoundException {
    final JdbcExportArgs options =
        optionsFromArgs(
            "--connectionUrl=jdbc:postgresql://some_db --table=some_table "
                + "--password=secret --useAvroLogicalTypes=true");

    Assertions.assertTrue(options.useAvroLogicalTypes());
  }

  @Test
  public void shouldRejectTimestampMicrosWithoutAvroLogicalTypes() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            optionsFromArgs(
                "--connectionUrl=jdbc:postgresql://some_db --table=some_table "
                    + "--password=secret --useTimestampMicros=true"));
  }

  @Test
  public void shouldConfigureTimestampMicrosWithAvroLogicalTypes()
      throws IOException, ClassNotFoundException {
    final JdbcExportArgs options =
        optionsFromArgs(
            "--connectionUrl=jdbc:postgresql://some_db --table=some_table "
                + "--password=secret --useTimestampMicros=true --useAvroLogicalTypes=true");

    Assertions.assertTrue(options.useTimestampMicros());
    Assertions.assertTrue(options.useAvroLogicalTypes());
  }

  @Test
  public void shouldConfigureAvroDoc() throws IOException, ClassNotFoundException {
    final JdbcExportArgs options =
        optionsFromArgs(
            "--connectionUrl=jdbc:postgresql://some_db --table=some_table "
                + "--password=secret --avroDoc=somedoc");

    Assertions.assertEquals(Optional.of("somedoc"), options.avroDoc());
  }

  @Test
  public void shouldConfigureAvroSchemaNamespace() throws IOException, ClassNotFoundException {
    final JdbcExportArgs options =
        optionsFromArgs(
            "--connectionUrl=jdbc:postgresql://some_db --table=some_table "
                + "--password=secret --avroSchemaNamespace=ns");

    Assertions.assertEquals("ns", options.avroSchemaNamespace());
  }

  @Test
  public void shouldDefaultOutputDataOnlyToFalse() throws IOException, ClassNotFoundException {
    final PipelineOptions defaultDataOnlyoptions =
        JdbcAvroJob.buildPipelineOptions(
            new String[] {
              "--connectionUrl=jdbc:postgresql://some_db", "--table=some_table", "--password=secret"
            });

    Assertions.assertEquals(false, defaultDataOnlyoptions.as(OutputOptions.class).getDataOnly());
  }

  @Test
  public void shouldConfigureOutputDataOnly() throws IOException, ClassNotFoundException {
    final PipelineOptions options =
        JdbcAvroJob.buildPipelineOptions(
            new String[] {
              "--connectionUrl=jdbc:postgresql://some_db",
              "--table=some_table",
              "--password=secret",
              "--dataOnly"
            });

    Assertions.assertEquals(true, options.as(OutputOptions.class).getDataOnly());
  }

  @Test
  public void shouldConfigureFetchSize() throws IOException, ClassNotFoundException {
    final JdbcExportArgs options =
        optionsFromArgs(
            "--connectionUrl=jdbc:postgresql://some_db --table=some_table "
                + "--password=secret --fetchSize=1234");

    Assertions.assertEquals(1234, options.jdbcAvroOptions().fetchSize());
  }

  @Test
  public void shouldSupportMonthlyPartitionPeriod() throws IOException, ClassNotFoundException {
    // Given handling ChronoUnit.MONTHS is not always simple
    // https://stackoverflow.com/q/39907925/1046584
    final JdbcExportArgs options =
        optionsFromArgs(
            "--connectionUrl=jdbc:postgresql://some_db --table=some_table "
                + "--password=secret --partitionPeriod=P1M --partition=2050-12");

    Assertions.assertEquals(Period.ofMonths(1), options.queryBuilderArgs().partitionPeriod());
  }

  @Test
  public void shouldConfigureDeflateCodec() throws IOException, ClassNotFoundException {
    final JdbcExportArgs options =
        optionsFromArgs(
            "--connectionUrl=jdbc:postgresql://some_db --table=some_table "
                + "--password=secret --avroCodec=deflate7");

    Assertions.assertEquals("deflate7", options.jdbcAvroOptions().avroCodec());
    Assertions.assertEquals(
        CodecFactory.deflateCodec(7).toString(),
        options.jdbcAvroOptions().getCodecFactory().toString());
  }

  @Test
  public void shouldConfigureZstandardCodec() throws IOException, ClassNotFoundException {
    final JdbcExportArgs options =
        optionsFromArgs(
            "--connectionUrl=jdbc:postgresql://some_db --table=some_table "
                + "--password=secret --avroCodec=zstandard9");

    Assertions.assertEquals("zstandard9", options.jdbcAvroOptions().avroCodec());
    Assertions.assertEquals(
        CodecFactory.zstandardCodec(9).toString(),
        options.jdbcAvroOptions().getCodecFactory().toString());
  }

  @Test
  public void shouldConfigureSnappyCodec() throws IOException, ClassNotFoundException {
    final JdbcExportArgs options =
        optionsFromArgs(
            "--connectionUrl=jdbc:postgresql://some_db --table=some_table "
                + "--password=secret --avroCodec=snappy");

    Assertions.assertEquals("snappy", options.jdbcAvroOptions().avroCodec());
    Assertions.assertEquals(
        CodecFactory.snappyCodec().toString(),
        options.jdbcAvroOptions().getCodecFactory().toString());
  }

  @Test
  public void shouldConfiguraPreCommands() throws IOException, ClassNotFoundException {
    final JdbcExportArgs options =
        optionsFromArgs(
            new String[] {
              "--connectionUrl=jdbc:postgresql://some_db;",
              "--password=secret",
              "--table=some_table",
              "--preCommand=set foo='1'",
              "--preCommand=set bar=2"
            });

    Assertions.assertEquals("set foo='1'", options.jdbcAvroOptions().preCommand().get(0));
    Assertions.assertEquals("set bar=2", options.jdbcAvroOptions().preCommand().get(1));
  }

  @Test
  public void shouldFailOnInvalidAvroCodec() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            optionsFromArgs(
                "--connectionUrl=jdbc:postgresql://some_db --table=some_table "
                    + "--password=secret --avroCodec=lzma"));
  }

  @Test
  public void shouldFailOnQueryParallelismWithNoSplitColumn() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            optionsFromArgs(
                "--connectionUrl=jdbc:postgresql://some_db "
                    + "--table=some_table --password=secret --queryParallelism=10"));
  }

  @Test
  public void shouldFailOnSplitColumnWithNoQueryParallelism() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            optionsFromArgs(
                "--connectionUrl=jdbc:postgresql://some_db "
                    + "--table=some_table --password=secret --splitColumn=id"));
  }

  @Test
  public void shouldFailOnZeroQueryParallelism() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            optionsFromArgs(
                "--connectionUrl=jdbc:postgresql://some_db "
                    + "--table=some_table --password=secret"
                    + " --queryParallelism=0 --splitColumn=id"));
  }

  @Test
  public void shouldFailOnNegativeQueryParallelism() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            optionsFromArgs(
                "--connectionUrl=jdbc:postgresql://some_db --table=some_table "
                    + "--password=secret --queryParallelism=-5 --splitColumn=id"));
  }
}
