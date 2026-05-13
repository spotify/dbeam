# Changelog

## [Unreleased]

### Added

- **dbeam-parquet module**: New module for exporting SQL databases directly to Parquet files,
  complementing the existing Avro export in dbeam-core.
  - `JdbcParquetJob`: Main entry point for Parquet exports, reuses all shared infrastructure
    from dbeam-core (JDBC connectors, query building, partitioning, parallel queries, Beam runners).
  - `JdbcParquetSchema`: Converts JDBC ResultSetMetaData to Parquet MessageType schema with
    full SQL type mapping including logical types (TIMESTAMP_MILLIS, UUID).
  - `JdbcParquetWriteSupport`: Writes ResultSet rows directly to Parquet RecordConsumer
    without intermediate objects.
  - `ChannelOutputFile`: OutputFile implementation backed by WritableByteChannel, bypassing
    Hadoop for file I/O.
  - Parquet compression codecs: snappy, gzip, zstd, lz4, uncompressed via `--parquetCodec`
    option (also auto-maps from `--avroCodec`).
  - SQL ARRAY columns use Parquet 3-level LIST type with typed elements (INT32, INT64, FLOAT,
    DOUBLE, BOOLEAN, STRING) inferred from column type name.
  - Parquet file footer includes `parquet.avro.schema` key with the Avro schema JSON, generated
    via JdbcAvroSchema for full compatibility with Spark, Hive, and BigQuery.
  - `_AVRO_SCHEMA.avsc` file saved alongside `_PARQUET_SCHEMA.json` in output directory.
  - `--parquetSchemaFilePath`: Input schema override (Parquet MessageType text format).
  - `--rowGroupSize` (default 128MB) and `--pageSize` (default 1MB) options.
  - `BeamJdbcParquetSchema`: Exposes schema creation timing as Beam metric.
  - `PsqlParquetJob`: PostgreSQL-specific job with replication lag check.
  - `BenchJdbcParquetJob`: Benchmarking job for multi-execution performance testing.
  - End-to-end test support in `e2e/e2e.sh` with `parquet-tools` validation.
  - Comprehensive test suite (90+ tests) covering schema conversion, record roundtrips,
    typed arrays, null handling, mock-based PostgreSQL type tests, and footer metadata.

### Changed

- Parent POM: Shade plugin config parameterized via `${dbeam.mainClass}` property,
  eliminating duplication between dbeam-core and dbeam-parquet.
- `PsqlReplicationCheck.validateOptions()` made public for cross-module access.
- dbeam-core: Added test-jar packaging for test helper sharing.
- CI: Codecov upload includes dbeam-parquet coverage. Parquet-tools installed for e2e validation.
- e2e: PostgreSQL upgraded from 16 to 18. Parquet test suite runs alongside Avro suite.
- Documentation: README updated with Parquet usage examples, library dependencies, and feature list.
  New `docs/parquet-type-conversion.md` with full type mapping table.
