/*-
 * -\-\-
 * DBeam Core
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.CountingOutputStream;

import com.spotify.dbeam.args.JdbcAvroArgs;

import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.DynamicAvroDestinations;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.ShardNameTemplate;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JdbcAvroIO {

  private static final String DEFAULT_SHARD_TEMPLATE = ShardNameTemplate.INDEX_OF_MAX;

  public static PTransform<PCollection<String>, WriteFilesResult<Void>> createWrite(
      String filenamePrefix, String filenameSuffix, Schema schema,
      JdbcAvroArgs jdbcAvroArgs) {
    filenamePrefix = filenamePrefix.replaceAll("/+$", "") + "/part";
    ValueProvider<ResourceId> prefixProvider =
        StaticValueProvider.of(FileBasedSink.convertToFileResourceIfPossible(filenamePrefix));
    FileBasedSink.FilenamePolicy filenamePolicy =
        DefaultFilenamePolicy.fromStandardParameters(
            prefixProvider,
            DEFAULT_SHARD_TEMPLATE,
            filenameSuffix,
            false);

    final DynamicAvroDestinations<String, Void, String>
        destinations =
        AvroIO.constantDestinations(filenamePolicy, schema, ImmutableMap.of(),
                                    // since Beam does not support zstandard
                                    CodecFactory.nullCodec(),
                                    SerializableFunctions.identity());
    final FileBasedSink<String, Void, String> sink = new JdbcAvroSink<>(
        prefixProvider,
        destinations,
        jdbcAvroArgs);
    return WriteFiles.to(sink);
  }


  static class JdbcAvroSink<UserT> extends FileBasedSink<UserT, Void, String> {

    private final DynamicAvroDestinations<?, Void, String> dynamicDestinations;
    private final JdbcAvroArgs jdbcAvroArgs;

    JdbcAvroSink(ValueProvider<ResourceId> filenamePrefix,
                            DynamicAvroDestinations<UserT, Void, String> dynamicDestinations,
                            JdbcAvroArgs jdbcAvroArgs) {
      super(filenamePrefix, dynamicDestinations, Compression.UNCOMPRESSED);
      this.dynamicDestinations = dynamicDestinations;
      this.jdbcAvroArgs = jdbcAvroArgs;
    }

    @Override
    public WriteOperation<Void, String> createWriteOperation() {
      return new JdbcAvroWriteOperation(this, dynamicDestinations, jdbcAvroArgs);
    }
  }

  private static class JdbcAvroWriteOperation
      extends FileBasedSink.WriteOperation<Void, String> {

    private final DynamicAvroDestinations<?, Void, String> dynamicDestinations;
    private final JdbcAvroArgs jdbcAvroArgs;

    private JdbcAvroWriteOperation(FileBasedSink<?, Void, String> sink,
                                   DynamicAvroDestinations<?, Void, String> dynamicDestinations,
                                   JdbcAvroArgs jdbcAvroArgs) {

      super(sink);
      this.dynamicDestinations = dynamicDestinations;
      this.jdbcAvroArgs = jdbcAvroArgs;
    }

    @Override
    public FileBasedSink.Writer<Void, String> createWriter() {
      return new JdbcAvroWriter(this, dynamicDestinations, jdbcAvroArgs);
    }
  }

  private static class JdbcAvroWriter extends FileBasedSink.Writer<Void, String> {
    private final Logger logger = LoggerFactory.getLogger(JdbcAvroWriter.class);
    private final int syncInterval = DataFileConstants.DEFAULT_SYNC_INTERVAL * 16; // 1 MB
    private final DynamicAvroDestinations<?, Void, String> dynamicDestinations;
    private final JdbcAvroArgs jdbcAvroArgs;
    private DataFileWriter<GenericRecord> dataFileWriter;
    private Connection connection;
    private JdbcAvroMetering metering;
    private CountingOutputStream countingOutputStream;

    JdbcAvroWriter(FileBasedSink.WriteOperation<Void, String> writeOperation,
                          DynamicAvroDestinations<?, Void, String> dynamicDestinations,
                          JdbcAvroArgs jdbcAvroArgs) {
      super(writeOperation, MimeTypes.BINARY);
      this.dynamicDestinations = dynamicDestinations;
      this.jdbcAvroArgs = jdbcAvroArgs;
      this.metering = JdbcAvroMetering.create();
    }

    public Void getDestination() {
      return null;
    }

    @SuppressWarnings("deprecation") // uses internal test functionality.
    @Override
    protected void prepareWrite(WritableByteChannel channel) throws Exception {
      logger.info("jdbcavroio : Preparing write...");
      connection = jdbcAvroArgs.jdbcConnectionConfiguration().createConnection();
      Void destination = getDestination();
      Schema schema = dynamicDestinations.getSchema(destination);
      dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(schema))
          .setCodec(jdbcAvroArgs.getCodecFactory())
          .setSyncInterval(syncInterval);
      dataFileWriter.setMeta("created_by", this.getClass().getCanonicalName());
      this.countingOutputStream = new CountingOutputStream(Channels.newOutputStream(channel));
      dataFileWriter.create(schema, this.countingOutputStream);
      logger.info("jdbcavroio : Write prepared");
    }

    private ResultSet executeQuery(String query) throws Exception {
      checkArgument(connection != null,
                    "JDBC connection was not properly created");
      PreparedStatement statement = connection.prepareStatement(
          query,
          ResultSet.TYPE_FORWARD_ONLY,
          ResultSet.CONCUR_READ_ONLY);
      statement.setFetchSize(jdbcAvroArgs.fetchSize());
      if (jdbcAvroArgs.statementPreparator() != null) {
        jdbcAvroArgs.statementPreparator().setParameters(statement);
      }

      if (jdbcAvroArgs.preCommand() != null && jdbcAvroArgs.preCommand().size() > 0) {
        Statement stmt = connection.createStatement();
        for (String command : jdbcAvroArgs.preCommand()) {
          stmt.execute(command);
        }
      }

      long startTime = System.nanoTime();
      logger.info(
          "jdbcavroio : Executing query with fetchSize={} (this might take a few minutes) ...",
          statement.getFetchSize());
      ResultSet resultSet = statement.executeQuery();
      this.metering.exposeExecuteQueryMs((System.nanoTime() - startTime) / 1000000L);
      checkArgument(resultSet != null,
                    "JDBC resultSet was not properly created");
      return resultSet;
    }

    @Override
    public void write(String query) throws Exception {
      checkArgument(dataFileWriter != null,
                    "Avro DataFileWriter was not properly created");
      logger.info("jdbcavroio : Starting write...");
      try (ResultSet resultSet = executeQuery(query)) {
        metering.startWriteMeter();
        final JdbcAvroRecordConverter converter = JdbcAvroRecordConverter.create(resultSet);
        while (resultSet.next()) {
          dataFileWriter.appendEncoded(converter.convertResultSetIntoAvroBytes());
          this.metering.incrementRecordCount();
        }
        this.dataFileWriter.flush();
        this.metering.exposeWriteElapsed();
        this.metering.exposeWrittenBytes(this.countingOutputStream.getCount());
      }
    }

    @Override
    protected void finishWrite() throws Exception {
      logger.info("jdbcavroio : Closing connection, flushing writer...");
      if (connection != null) {
        connection.close();
      }
      if (dataFileWriter != null) {
        dataFileWriter.close();
      }
      logger.info("jdbcavroio : Write finished");
    }
  }

}
