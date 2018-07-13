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
import com.google.common.collect.ImmutableMap;

import org.apache.avro.Schema;
import org.apache.avro.file.Codec;
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
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.annotation.Nullable;
import javax.sql.DataSource;

import static com.google.common.base.Preconditions.checkArgument;

public class JdbcAvroIO {

  public abstract static class Write {

    private static final String DEFAULT_SHARD_TEMPLATE = ShardNameTemplate.INDEX_OF_MAX;

    public static PTransform<PCollection<String>, WriteFilesResult<Void>> createWrite(
        String filenamePrefix, String filenameSuffix, Schema schema,
        JdbcAvroOptions jdbcAvroOptions) {
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
                                      CodecFactory.deflateCodec(jdbcAvroOptions.getDeflateCompressionLevel()),
                                      SerializableFunctions.identity());
      final FileBasedSink<String, Void, String> sink = new JdbcAvroSink<>(
          prefixProvider,
          destinations,
          jdbcAvroOptions);
      return WriteFiles.to(sink);
    }

  }

  static class JdbcAvroSink<UserT> extends FileBasedSink<UserT, Void, String> {

    private final DynamicAvroDestinations<?, Void, String> dynamicDestinations;
    private final JdbcAvroOptions jdbcAvroOptions;

    JdbcAvroSink(ValueProvider<ResourceId> filenamePrefix,
                            DynamicAvroDestinations<UserT, Void, String> dynamicDestinations,
                            JdbcAvroOptions jdbcAvroOptions) {
      super(filenamePrefix, dynamicDestinations, Compression.UNCOMPRESSED);
      this.dynamicDestinations = dynamicDestinations;
      this.jdbcAvroOptions = jdbcAvroOptions;
    }

    @Override
    public WriteOperation<Void, String> createWriteOperation() {
      return new JdbcAvroWriteOperation(this, dynamicDestinations, jdbcAvroOptions);
    }
  }


  private static class JdbcAvroWriteOperation extends FileBasedSink.WriteOperation<Void, String> {

    private final DynamicAvroDestinations<?, Void, String> dynamicDestinations;
    private final JdbcAvroOptions jdbcAvroOptions;

    private JdbcAvroWriteOperation(FileBasedSink<?, Void, String> sink,
                                   DynamicAvroDestinations<?, Void, String> dynamicDestinations,
                                   JdbcAvroOptions jdbcAvroOptions) {

      super(sink);
      this.dynamicDestinations = dynamicDestinations;
      this.jdbcAvroOptions = jdbcAvroOptions;
    }

    @Override
    public FileBasedSink.Writer<Void, String> createWriter() {
      return new JdbcAvroWriter(this, dynamicDestinations, jdbcAvroOptions);
    }
  }

  private static class JdbcAvroWriter extends FileBasedSink.Writer<Void, String> {
    private static final int COUNTER_REPORT_EVERY = 100000;
    private static final int LOG_EVERY = 100000;
    private final Logger logger = LoggerFactory.getLogger(JdbcAvroWriter.class);
    private final DynamicAvroDestinations<?, Void, String> dynamicDestinations;
    private final JdbcAvroOptions jdbcAvroOptions;
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

    JdbcAvroWriter(FileBasedSink.WriteOperation<Void, String> writeOperation,
                          DynamicAvroDestinations<?, Void, String> dynamicDestinations,
                          JdbcAvroOptions jdbcAvroOptions) {
      super(writeOperation, MimeTypes.BINARY);
      this.dynamicDestinations = dynamicDestinations;
      this.jdbcAvroOptions = jdbcAvroOptions;
      this.syncInterval = DataFileConstants.DEFAULT_SYNC_INTERVAL * 16; // 1 MB
    }

    public Void getDestination() {
      return null;
    }

    @SuppressWarnings("deprecation") // uses internal test functionality.
    @Override
    protected void prepareWrite(WritableByteChannel channel) throws Exception {
      logger.info("jdbcavroio : Preparing write...");
      connection = jdbcAvroOptions.getDataSourceConfiguration().getConnection();
      Void destination = getDestination();
      CodecFactory codec = dynamicDestinations.getCodec(destination);
      Schema schema = dynamicDestinations.getSchema(destination);
      dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(schema))
          .setCodec(codec)
          .setSyncInterval(syncInterval);
      dataFileWriter.setMeta("created_by", this.getClass().getCanonicalName());
      dataFileWriter.create(schema, Channels.newOutputStream(channel));
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
      statement.setFetchSize(jdbcAvroOptions.getFetchSize());
      if (jdbcAvroOptions.getStatementPreparator() != null) {
        jdbcAvroOptions.getStatementPreparator().setParameters(statement);
      }

      long startTime = System.currentTimeMillis();
      logger.info("jdbcavroio : Executing query with fetchSize={} (this can take a few minutes) ...",
                  statement.getFetchSize());
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
      Schema schema = dynamicDestinations.getSchema(getDestination());
      RowMapper rowMapper = jdbcAvroOptions.getAvroRowMapper();
      try (ResultSet resultSet = executeQuery(query)) {
        checkArgument(resultSet != null,
                      "JDBC resultSet was not properly created");
        this.writeIterateStartTime = System.currentTimeMillis();
        while (resultSet.next()) {
          GenericRecord genericRecord = rowMapper.convert(resultSet, schema);
          this.dataFileWriter.append(genericRecord);
          incrementRecordCount();
        }
        exposeMetrics(System.currentTimeMillis() - this.writeIterateStartTime);
      }
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
        if (elapsedMs != 0) {
          this.rowsPerMinute.set((60 * 1000L) * rowCount / elapsedMs);
        }
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

  private static class DefaultRowMapper implements RowMapper {

    @Override
    public GenericRecord convert(ResultSet resultSet, Schema schema) throws Exception {
      return JdbcAvroConversions.convertResultSetIntoAvroRecord(schema, resultSet);
    }
  }

  @AutoValue
  public abstract static class JdbcAvroOptions implements Serializable {
    abstract DataSourceConfiguration getDataSourceConfiguration();
    @Nullable abstract StatementPreparator getStatementPreparator();
    abstract RowMapper getAvroRowMapper();
    abstract int getFetchSize();
    abstract int getDeflateCompressionLevel();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setDataSourceConfiguration(DataSourceConfiguration dataSourceConfiguration);
      abstract Builder setStatementPreparator(StatementPreparator statementPreparator);
      abstract Builder setAvroRowMapper(RowMapper avroRowMapper);
      abstract Builder setFetchSize(int fetchSize);
      abstract Builder setDeflateCompressionLevel(int codecFactory);
      abstract JdbcAvroOptions build();
    }

    public static JdbcAvroOptions create(DataSourceConfiguration dataSourceConfiguration,
                                         int fetchSize, int deflateCompressionLevel) {
      return new AutoValue_JdbcAvroIO_JdbcAvroOptions.Builder()
          .setDataSourceConfiguration(dataSourceConfiguration)
          .setAvroRowMapper(new DefaultRowMapper())
          .setFetchSize(fetchSize)
          .setDeflateCompressionLevel(deflateCompressionLevel)
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
        Class.forName(getDriverClassName());
        return DriverManager.getConnection(getUrl(), getUsername(), getPassword());
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
