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
  - Parquet compression codecs: snappy (default), gzip, zstd, zstd1 .. zstd22 (Zstandard
    level), lz4, uncompressed via `--parquetCodec`, validated before any output is written.
    `--avroCodec` is not mapped to a Parquet codec; a non-default value logs a warning.
  - STRUCT, REF, REF_CURSOR and DATALINK columns fail schema generation with the same messages
    as the Avro export.
  - SQL ARRAY columns use Parquet 3-level LIST type with typed elements (INT32, INT64, FLOAT,
    DOUBLE, BOOLEAN, BINARY, STRING) inferred from column type name. Other element types
    (timestamp, date, numeric, json, ...) fail schema generation naming the column and element
    type (use `--arrayMode=bytes`) instead of being written as strings. Multidimensional
    arrays fail with an error naming the column before anything of the value is written.
  - Parquet file footer includes `parquet.avro.schema` key with the Avro schema JSON, derived
    from the Parquet schema used for writing (also for `--parquetSchemaFilePath`) with
    parquet-avro's `AvroSchemaConverter` (`parquet.avro.add-list-element-records=false`), so that
    it always matches the written columns and `AvroParquetReader` can decode the files: LIST items
    are nullable (`array<["null", T]>`, no wrapper record), UUID is `string`/`uuid`,
    typed_first_row array element types follow the Parquet schema, and a NULL first-row ARRAY no
    longer drops the key. Only the top-level record's name, namespace and doc are set by dbeam;
    no record or per-field properties are added. If the Avro schema cannot be derived (e.g. INT96
    or invalid Avro names in a schema file), the key is omitted and the export continues.
  - `_AVRO_SCHEMA.avsc` file saved alongside `_PARQUET_SCHEMA.json` in output directory,
    identical to the footer schema.
  - `--parquetSchemaFilePath`: Input schema override (Parquet MessageType text format),
    validated against the JDBC-derived schema before export (renames and compatible annotations
    only; fails with `FailedValidationException` otherwise). `--avroSchemaFilePath` is rejected
    on Parquet jobs with an `IllegalArgumentException` pointing to `--parquetSchemaFilePath`.
  - `--parquetNativeTypes` (default false, requires `--useLogicalTypes`): DATE as `int32 (DATE)`,
    TIME/TIME_WITH_TIMEZONE as `TIME(MILLIS|MICROS, true)` and DECIMAL/NUMERIC with JDBC
    precision 1..38 as `fixed_len_byte_array (DECIMAL(p,s))`, with a string fallback for
    unbounded precision (e.g. PostgreSQL NUMERIC). Values that do not fit fail the export.
    Schema files may use these annotations only with the flag.
  - `--rowGroupSize` (default 128MB) and `--pageSize` (default 1MB) options.
  - `--useTimestampMicros` (requires `--useLogicalTypes`) writes TIMESTAMP, DATE, TIME and
    TIME_WITH_TIMEZONE columns as `TIMESTAMP(MICROS, isAdjustedToUTC=true)` epoch microseconds
    instead of `TIMESTAMP(MILLIS, ...)`, and the footer Avro schema and `_AVRO_SCHEMA.avsc` use
    `timestamp-micros` to match. With `--parquetSchemaFilePath`, schema validation rejects a
    TIMESTAMP field whose unit disagrees with `--useTimestampMicros`. The flag alone decides the
    written unit; unannotated `int64` fields in a schema file are written in the flag's unit.
  - `JdbcParquetIO` closes the PreparedStatement and `--preCommand` Statement of every query, and
    the JDBC connection when preparing the writer or a query fails (`--preCommand` still runs
    before every query, as in `JdbcAvroIO`).
  - `BeamJdbcParquetSchema`: Exposes schema creation timing as Beam metric.
  - `bytesWritten` metric (`JdbcParquetMetering`): total bytes of the output files, counted once
    per file after the writer is closed (last row group and footer included), also when one
    writer handles several queries.
  - `PsqlParquetJob`: PostgreSQL-specific job with replication lag check.
  - `BenchJdbcParquetJob`: Benchmarking job for multi-execution performance testing.
  - End-to-end test support in `e2e/e2e.sh`: the Parquet suite mirrors the Avro runs (codecs,
    parallel queries, array modes) plus logical types, timestamp micros, uuid,
    `--parquetNativeTypes` and `--dataOnly`, and `e2e/VerifyParquetOutput.java` reads every
    output file of every run through its footer Avro schema (`AvroParquetReader`), checking row
    counts, `row_number` coverage, key values and that the footer equals `_AVRO_SCHEMA.avsc`.
  - Comprehensive test suite (90+ tests) covering schema conversion, record roundtrips,
    typed arrays, null handling, mock-based PostgreSQL type tests, and footer metadata.

- dbeam-core: `--avroSchemaFilePath` schemas are checked against `--useTimestampMicros`; a
  `timestamp-millis`/`timestamp-micros` field disagreeing with the flag, or any
  `local-timestamp-*`/`timestamp-nanos` field, fails fast (in `prepareExport`, before any output)
  with `FailedValidationException` (exit code 50), the same exception and exit code as Parquet
  schema-file validation. Pure option conflicts stay `IllegalArgumentException` (exit code 43).
- dbeam-core: `--avroSchemaFilePath` schemas are validated against the JDBC metadata (one
  `LIMIT 1` query in `JdbcAvroJob.prepareExport`): field count, `["null", T]` unions, Avro types
  including array items and their nullability, and logical types must match what the writer
  emits; renames and doc/namespace changes are allowed (`AvroSchemaValidator`).

- Avro and Parquet: columns whose names collide after normalization to `[A-Za-z0-9_]` (e.g.
  `a-b` and `a_b`) fail schema generation with an error listing the colliding columns (Avro
  previously failed with a generic duplicate field error; Parquet silently produced a schema
  with duplicate names).

### Changed

- Minimum supported JDK raised from 11 to 17.
- An invalid `--arrayMode` value fails with `IllegalArgumentException` (exit code 43, like other
  invalid arguments) listing the allowed values, instead of a plain `RuntimeException` (49).
- dbeam-core: Multidimensional ARRAY values fail with an error naming the column and suggesting
  `--arrayMode=bytes`, instead of the generic "Value of type ... is not supported" error.
- dbeam-core: Avro `bytesWritten` metric counts each output file once, after the writer is
  closed (last block included), instead of the cumulative file size after each query, which
  over-counted when one writer handled several queries.
- dbeam-core: `JdbcMetering` and `BenchJdbcJob` extracted as format-agnostic base classes;
  `JdbcAvroMetering` keeps its metric namespace, and Parquet metrics use `JdbcParquetMetering`.
- Parent POM: Shade plugin config parameterized via `${dbeam.mainClass}` property,
  eliminating duplication between dbeam-core and dbeam-parquet.
- `PsqlReplicationCheck.validateOptions()` made public for cross-module access.
- dbeam-core: Added test-jar packaging for test helper sharing.
- CI: Codecov upload includes dbeam-parquet coverage.
- e2e: PostgreSQL upgraded from 16 to 18. Parquet test suite runs alongside Avro suite.
- Documentation: README updated with Parquet usage examples, library dependencies, and feature list.
  New `docs/parquet-type-conversion.md` with full type mapping table.
