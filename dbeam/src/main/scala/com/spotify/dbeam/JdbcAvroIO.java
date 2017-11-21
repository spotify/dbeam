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

package com.spotify.dbeam;

import com.google.auto.value.AutoValue;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.ShardNameTemplate;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.annotation.Nullable;
import javax.sql.DataSource;

import static com.google.common.base.Preconditions.checkArgument;

public class JdbcAvroIO {
  private static final String DEFAULT_CODEC = "deflate";

  public abstract static class Write {

    private static final String DEFAULT_SHARD_TEMPLATE = ShardNameTemplate.INDEX_OF_MAX;

    public static PTransform<PCollection<String>, PDone> createWrite(
        String filenamePrefix, String filenameSuffix, Schema schema,
        JdbcAvroOptions jdbcAvroOptions) {
      filenamePrefix = filenamePrefix.replaceAll("/+$", "") + "/part";
      ValueProvider<ResourceId> prefixProvider =
          StaticValueProvider.of(FileBasedSink.convertToFileResourceIfPossible(filenamePrefix));
      FileBasedSink.FilenamePolicy usedFilenamePolicy =
          DefaultFilenamePolicy.constructUsingStandardParameters(
              prefixProvider, DEFAULT_SHARD_TEMPLATE, filenameSuffix, false);
      return WriteFiles.to(
          new JdbcAvroSink(
              prefixProvider,
              usedFilenamePolicy,
              jdbcAvroOptions,
              AvroCoder.of(GenericRecord.class, schema)));
    }

  }

  static class JdbcAvroSink extends FileBasedSink<String> {

    private final JdbcAvroOptions jdbcAvroOptions;
    private final AvroCoder<GenericRecord> coder;

    JdbcAvroSink(ValueProvider<ResourceId> filenamePrefix,
                 FilenamePolicy usedFilenamePolicy,
                 JdbcAvroOptions jdbcAvroOptions,
                 AvroCoder<GenericRecord> coder) {
      super(filenamePrefix, usedFilenamePolicy);
      this.jdbcAvroOptions = jdbcAvroOptions;
      this.coder = coder;
    }

    @Override
    public WriteOperation<String> createWriteOperation() {
      return new JdbcAvroWriteOperation(this, jdbcAvroOptions, coder);
    }
  }


  private static class JdbcAvroWriteOperation extends org.apache.beam.sdk.io.FileBasedSink.WriteOperation<String> {
    private final AvroCoder<GenericRecord> coder;
    private final JdbcAvroOptions jdbcAvroOptions;

    private JdbcAvroWriteOperation(JdbcAvroSink sink,
                                   JdbcAvroOptions jdbcAvroOptions,
                                   AvroCoder<GenericRecord> coder) {

      super(sink);
      this.coder = coder;
      this.jdbcAvroOptions = jdbcAvroOptions;
    }

    @Override
    public FileBasedSink.Writer<String> createWriter() throws Exception {
      return new JdbcAvroWriter(this, jdbcAvroOptions, coder);
    }
  }

  private static class JdbcAvroWriter extends org.apache.beam.sdk.io.FileBasedSink.Writer<String> {
    private static final int FETCH_SIZE = 10000;
    private static final int COUNTER_REPORT_EVERY = 100000;
    private static final int LOG_EVERY = 100000;
    private final Logger logger = LoggerFactory.getLogger(JdbcAvroWriter.class);
    private final AvroCoder<GenericRecord> coder;
    private final JdbcAvroOptions jdbcAvroOptions;
    private final CodecFactory codec;
    private final int syncInterval;
    private DataFileWriter<GenericRecord> dataFileWriter;
    private Connection connection;
    private Counter recordCount =
        Metrics.counter(this.getClass().getCanonicalName(),"recordCount");
    private Counter executeQueryElapsedMs =
        Metrics.counter(this.getClass().getCanonicalName(), "executeQueryElapsedMs");
    private Counter writeElapsedMs =
        Metrics.counter(this.getClass().getCanonicalName(), "writeElapsedMs");
    private Gauge msPerMillionRows =
        Metrics.gauge(this.getClass().getCanonicalName(), "msPerMillionRows");
    private Gauge rowsPerMinute =
        Metrics.gauge(this.getClass().getCanonicalName(), "rowsPerMinute");
    private int rowCount;
    private long writeIterateStartTime;

    public JdbcAvroWriter(org.apache.beam.sdk.io.FileBasedSink.WriteOperation<String> writeOperation,
                          JdbcAvroOptions jdbcAvroOptions,
                          AvroCoder<GenericRecord> coder) {
      super(writeOperation, MimeTypes.BINARY);
      this.jdbcAvroOptions = jdbcAvroOptions;
      this.coder = coder;
      //this.codec = CodecFactory.fromString(this.jdbcAvroOptions.getCodec());
      this.codec = CodecFactory.deflateCodec(1);
      this.syncInterval = DataFileConstants.DEFAULT_SYNC_INTERVAL * 16; // 1 MB
    }

    @SuppressWarnings("deprecation") // uses internal test functionality.
    @Override
    protected void prepareWrite(WritableByteChannel channel) throws Exception {
      logger.info("jdbcavroio : Preparing write...");
      connection = jdbcAvroOptions.getDataSourceConfiguration().getConnection();
      dataFileWriter =
          new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(coder.getSchema()))
              .setCodec(codec)
              .setSyncInterval(syncInterval);
      dataFileWriter.setMeta("created_by", this.getClass().getCanonicalName());
      dataFileWriter.create(coder.getSchema(), Channels.newOutputStream(channel));
      rowCount = 0;
      logger.info("jdbcavroio : Write prepared");
    }

    private ResultSet executeQuery(String query) throws Exception {
      checkArgument(connection != null,
                    "JDBC connection was not properly created");
      connection.setAutoCommit(false);
      PreparedStatement statement = connection.prepareStatement(
          query,
          ResultSet.TYPE_FORWARD_ONLY,
          ResultSet.CONCUR_READ_ONLY);
      statement.setFetchSize(FETCH_SIZE);
      if (jdbcAvroOptions.getStatementPreparator() != null) {
        jdbcAvroOptions.getStatementPreparator().setParameters(statement);
      }

      long startTime = System.currentTimeMillis();
      logger.info("jdbcavroio : Executing query (this can take a few minutes) ...");
      ResultSet resultSet = statement.executeQuery();
      long elapsed1 = System.currentTimeMillis() - startTime;
      logger.info(String.format("jdbcavroio : Execute query took %5.2f seconds",
                                elapsed1/1000.0));
      this.executeQueryElapsedMs.inc(elapsed1);
      return resultSet;
    }

    @Override
    public void write(String query) throws Exception {
      checkArgument(dataFileWriter != null,
                    "Avro DataFileWriter was not properly created");
      logger.info("jdbcavroio : Starting write...");
      try (ResultSet resultSet = executeQuery(query)) {
        checkArgument(resultSet != null,
                      "JDBC resultSet was not properly created");
        this.writeIterateStartTime = System.currentTimeMillis();
        while (resultSet.next()) {
          GenericRecord genericRecord =
              jdbcAvroOptions.getAvroRowMapper().convert(resultSet, coder.getSchema());
          writeRecord(genericRecord);
          incrementRecordCount();
        }
        exposeMetrics(System.currentTimeMillis() - this.writeIterateStartTime);
      }
    }

    private void writeRecord(GenericRecord genericRecord) throws IOException {
      this.dataFileWriter.append(genericRecord);
    }

    /**
     * Increment and report counters to Beam SDK and logs
     * To avoid slowing down the writes, counts are reported every x 1000s of rows
     * This exposes the job progress
     */
    private void incrementRecordCount() {
      this.rowCount++;
      if ((this.rowCount % COUNTER_REPORT_EVERY) == 0) {
        this.recordCount.inc(COUNTER_REPORT_EVERY);
        long elapsedMs = System.currentTimeMillis() - this.writeIterateStartTime;
        long msPerMillionRows = 1000000L * elapsedMs / rowCount,
             rowsPerMinute = (60*1000L) * rowCount / elapsedMs;
        this.msPerMillionRows.set(msPerMillionRows);
        this.rowsPerMinute.set(rowsPerMinute);
        if ((this.rowCount % LOG_EVERY) == 0) {
          logger.info(String.format(
              "jdbcavroio : Fetched # %08d rows at %08d rows per minute and %08d ms per M rows",
              rowCount, rowsPerMinute, msPerMillionRows));
        }
      }
    }

    private void exposeMetrics(long elapsedMs) {
      logger.info(String.format("jdbcavroio : Read %d rows, took %5.2f seconds",
                                rowCount, elapsedMs/1000.0));
      this.writeElapsedMs.inc(elapsedMs);
      if (rowCount > 0) {
        this.recordCount.inc((this.rowCount % COUNTER_REPORT_EVERY));
        this.msPerMillionRows.set(1000000L * elapsedMs / rowCount);
        this.rowsPerMinute.set((60*1000L) * rowCount / elapsedMs);
      }
    }

    @Override
    protected void finishWrite() throws Exception {
      logger.info("jdbcavroio : Closing connection, flushing writer...");
      if (connection != null) {
        connection.close();
      }
      if (dataFileWriter != null) {
        dataFileWriter.flush();
      }
      logger.info("jdbcavroio : Write finished");
    }
  }

  @AutoValue
  public abstract static class JdbcAvroOptions implements Serializable {
    abstract DataSourceConfiguration getDataSourceConfiguration();
    @Nullable abstract StatementPreparator getStatementPreparator();
    abstract String getCodec();
    abstract RowMapper getAvroRowMapper();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setDataSourceConfiguration(DataSourceConfiguration dataSourceConfiguration);
      abstract Builder setStatementPreparator(StatementPreparator statementPreparator);
      abstract Builder setCodec(String codec);
      abstract Builder setAvroRowMapper(RowMapper avroRowMapper);
      abstract JdbcAvroOptions build();
    }

    public static JdbcAvroOptions create(DataSourceConfiguration dataSourceConfiguration,
                                         RowMapper avroRowMapper) {
      return new AutoValue_JdbcAvroIO_JdbcAvroOptions.Builder()
          .setDataSourceConfiguration(dataSourceConfiguration)
          .setCodec(DEFAULT_CODEC)
          .setAvroRowMapper(avroRowMapper)
          .build();
    }
  }

  /**
   * A POJO describing a {@link DataSource}, either providing directly a {@link DataSource} or all
   * properties allowing to create a {@link DataSource}.
   */
  @AutoValue
  public abstract static class DataSourceConfiguration implements Serializable {
    @Nullable abstract String getDriverClassName();
    @Nullable abstract String getUrl();
    @Nullable abstract String getUsername();
    @Nullable abstract String getPassword();
    @Nullable abstract DataSource getDataSource();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setDriverClassName(String driverClassName);
      abstract Builder setUrl(String url);
      abstract Builder setUsername(String username);
      abstract Builder setPassword(String password);
      abstract Builder setDataSource(DataSource dataSource);
      abstract DataSourceConfiguration build();
    }

    public static DataSourceConfiguration create(DataSource dataSource) {
      checkArgument(dataSource != null,
          "DataSourceConfiguration.create(dataSource) called with null data source");
      checkArgument(dataSource instanceof Serializable,
          "DataSourceConfiguration.create(dataSource) called with "
              + "a dataSource not Serializable");
      return new AutoValue_JdbcAvroIO_DataSourceConfiguration.Builder()
          .setDataSource(dataSource)
          .build();
    }

    public static DataSourceConfiguration create(String driverClassName, String url) {
      checkArgument(driverClassName != null,
                    "DataSourceConfiguration.create(driverClassName, url) called "
                        + "with null driverClassName");
      checkArgument(url != null,
                    "DataSourceConfiguration.create(driverClassName, url) called "
                        + "with null url");
      return new AutoValue_JdbcAvroIO_DataSourceConfiguration.Builder()
          .setDriverClassName(driverClassName)
          .setUrl(url)
          .build();
    }

    public DataSourceConfiguration withUsername(String username) {
      return builder().setUsername(username).build();
    }

    public DataSourceConfiguration withPassword(String password) {
      return builder().setPassword(password).build();
    }

    Connection getConnection() throws Exception {
      if (getDataSource() != null) {
        return (getUsername() != null)
               ? getDataSource().getConnection(getUsername(), getPassword())
               : getDataSource().getConnection();
      } else {
        BasicDataSource basicDataSource = new BasicDataSource();
        basicDataSource.setDriverClassName(getDriverClassName());
        basicDataSource.setUrl(getUrl());
        basicDataSource.setUsername(getUsername());
        basicDataSource.setPassword(getPassword());
        return basicDataSource.getConnection();
      }
    }
  }

  public interface StatementPreparator extends Serializable {
    void setParameters(PreparedStatement preparedStatement) throws Exception;
  }

  public interface RowMapper extends Serializable {
    GenericRecord convert(ResultSet resultSet, Schema schema) throws Exception;
  }

}
