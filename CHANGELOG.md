# Changelog

## [Unreleased]

### Added

- `--useLogicalTypes`: format-neutral option controlling logical types in generated Avro and
  Parquet schemas. `--useAvroLogicalTypes` remains supported as a deprecated alias; setting
  both to conflicting values fails with an error.
- **dbeam-parquet module**: New module for exporting SQL databases directly to Parquet files,
  complementing the existing Avro export in dbeam-core.
  - `JdbcParquetJob`: Main entry point for Parquet exports, reuses all shared infrastructure
    from dbeam-core (JDBC connectors, query building, partitioning, parallel queries, Beam runners).
  - `JdbcParquetSchema`: Converts JDBC ResultSetMetaData to Parquet MessageType schema with
    full SQL type mapping including logical types (TIMESTAMP MILLIS/MICROS, UUID).
  - `JdbcParquetWriteSupport`: Writes ResultSet rows directly to Parquet RecordConsumer
    without intermediate objects.
  - `ChannelOutputFile`: OutputFile implementation backed by WritableByteChannel, bypassing
    Hadoop for file I/O.
  - Parquet compression codecs: snappy, gzip, zstd, lz4, uncompressed via `--parquetCodec`
    option (also auto-maps from `--avroCodec`).
  - SQL ARRAY columns use Parquet 3-level LIST type with typed elements (INT32, INT64, FLOAT,
    DOUBLE, BOOLEAN, BINARY, STRING) inferred from column type name.
  - Parquet file footer includes `parquet.avro.schema` key with the Avro schema JSON, derived
    from the Parquet schema used for writing (also for `--parquetSchemaFilePath`) so that it
    always matches the written columns and `AvroParquetReader` can decode the files: LIST items
    are nullable, UUID is `string`/`uuid`, typed_first_row array element types follow the Parquet
    schema, and a NULL first-row ARRAY no longer drops the key. Record namespace, doc and column
    properties follow dbeam-core's JdbcAvroSchema conventions. If the Avro schema cannot be
    derived (e.g. INT96 in a schema file), the key is omitted and the export continues.
  - `_AVRO_SCHEMA.avsc` file saved alongside `_PARQUET_SCHEMA.json` in output directory,
    identical to the footer schema.
  - `--parquetSchemaFilePath`: Input schema override (Parquet MessageType text format).
  - `--rowGroupSize` (default 128MB) and `--pageSize` (default 1MB) options.
  - `--useTimestampMicros` (requires `--useLogicalTypes`) writes TIMESTAMP, DATE, TIME and
    TIME_WITH_TIMEZONE columns as `TIMESTAMP(MICROS, isAdjustedToUTC=true)` epoch microseconds
    instead of `TIMESTAMP(MILLIS, ...)`, and the footer Avro schema and `_AVRO_SCHEMA.avsc` use
    `timestamp-micros` to match. With `--parquetSchemaFilePath`, the job fails fast if a
    TIMESTAMP field's unit disagrees with `--useTimestampMicros`.
  - `BeamJdbcParquetSchema`: Exposes schema creation timing as Beam metric.
  - `PsqlParquetJob`: PostgreSQL-specific job with replication lag check.
  - `BenchJdbcParquetJob`: Benchmarking job for multi-execution performance testing.
  - End-to-end test support in `e2e/e2e.sh` with `parquet-tools` validation.
  - Comprehensive test suite (90+ tests) covering schema conversion, record roundtrips,
    typed arrays, null handling, mock-based PostgreSQL type tests, and footer metadata.

### Changed

- Minimum supported JDK raised from 11 to 17.
- dbeam-core: `JdbcMetering` and `BenchJdbcJob` extracted as format-agnostic base classes;
  `JdbcAvroMetering` keeps its metric namespace, and Parquet metrics use `JdbcParquetMetering`.
- Parent POM: Shade plugin config parameterized via `${dbeam.mainClass}` property,
  eliminating duplication between dbeam-core and dbeam-parquet.
- `PsqlReplicationCheck.validateOptions()` made public for cross-module access.
- dbeam-core: Added test-jar packaging for test helper sharing.
- CI: Codecov upload includes dbeam-parquet coverage. Parquet-tools installed for e2e validation.
- e2e: PostgreSQL upgraded from 16 to 18. Parquet test suite runs alongside Avro suite.
- Documentation: README updated with Parquet usage examples, library dependencies, and feature list.
  New `docs/parquet-type-conversion.md` with full type mapping table.
