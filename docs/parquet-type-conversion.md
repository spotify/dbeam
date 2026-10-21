### Type Conversion Details: Java SQL ==> Parquet

Java SQL types (java.sql.Types.*) are converted to Parquet types according to the following table.
When applicable and `--useLogicalTypes` parameter is set to `true`, Parquet logical types are used.
`--useAvroLogicalTypes` is a deprecated alias of `--useLogicalTypes` and is still supported.

All columns are represented as `optional` (nullable) fields in the Parquet schema. Field names
are the column names with characters outside `[A-Za-z0-9_]` replaced by `_`; if two columns end
up with the same field name (e.g. `a-b` and `a_b`), the export fails listing the colliding
columns (use column aliases to fix it).

When `--useTimestampMicros` is also set (it requires `--useLogicalTypes`), TIMESTAMP, DATE,
TIME and TIME_WITH_TIMEZONE columns use `TIMESTAMP(MICROS, isAdjustedToUTC=true)` and values are
written as epoch microseconds, keeping the sub-millisecond part of `java.sql.Timestamp`. The Avro
schema stored in the `parquet.avro.schema` footer key then uses `timestamp-micros` to match.
`--useTimestampMicros` alone decides the written unit, also with `--parquetSchemaFilePath`: a
TIMESTAMP annotation in the schema file must agree with the flag or be absent (an unannotated
`int64` holds the flag's unit), otherwise the job fails (see
[Input schema file](#input-schema-file---parquetschemafilepath)).

| **Java SQL type**        | **Parquet physical type** | **Parquet logical type**        | **Comments**                          |
|--------------------------|---------------------------|---------------------------------|---------------------------------------|
| BIGINT                   | INT64                     |                                 |                                       |
| INTEGER                  | INT32                     |                                 | INT64 if column class is java.lang.Long (e.g. MySQL unsigned int) |
| SMALLINT                 | INT32                     |                                 |                                       |
| TINYINT                  | INT32                     |                                 |                                       |
| TIMESTAMP                | INT64                     | TIMESTAMP(MILLIS, isAdjustedToUTC=true) | Logical type only with `--useLogicalTypes`; MICROS with `--useTimestampMicros` |
| DATE                     | INT64                     | TIMESTAMP(MILLIS, isAdjustedToUTC=true) | Logical type only with `--useLogicalTypes`; MICROS with `--useTimestampMicros` |
| TIME                     | INT64                     | TIMESTAMP(MILLIS, isAdjustedToUTC=true) | Logical type only with `--useLogicalTypes`; MICROS with `--useTimestampMicros` |
| TIME_WITH_TIMEZONE       | INT64                     | TIMESTAMP(MILLIS, isAdjustedToUTC=true) | Logical type only with `--useLogicalTypes`; MICROS with `--useTimestampMicros` |
| BOOLEAN                  | BOOLEAN                   |                                 |                                       |
| BIT                      | BOOLEAN / BINARY          |                                 | BOOLEAN if precision <= 1, BINARY otherwise |
| BINARY                   | BINARY                    |                                 |                                       |
| VARBINARY                | BINARY                    |                                 |                                       |
| LONGVARBINARY            | BINARY                    |                                 |                                       |
| BLOB                     | BINARY                    |                                 |                                       |
| ARRAY                    | LIST (group)              | LIST                            | Typed elements (see table below)      |
| DOUBLE                   | DOUBLE                    |                                 |                                       |
| FLOAT                    | FLOAT                     |                                 |                                       |
| REAL                     | FLOAT                     |                                 |                                       |
| VARCHAR                  | BINARY                    | STRING (UTF8)                   |                                       |
| CHAR                     | BINARY                    | STRING (UTF8)                   |                                       |
| CLOB                     | BINARY                    | STRING (UTF8)                   |                                       |
| LONGNVARCHAR             | BINARY                    | STRING (UTF8)                   |                                       |
| LONGVARCHAR              | BINARY                    | STRING (UTF8)                   |                                       |
| NCHAR                    | BINARY                    | STRING (UTF8)                   |                                       |
| DECIMAL / NUMERIC        | BINARY                    | STRING (UTF8)                   | Exported as string representation; DECIMAL with `--parquetNativeTypes` (see below) |
| OTHER (uuid)             | FIXED_LEN_BYTE_ARRAY(16)  | UUID                            | Only with `--useLogicalTypes`; otherwise STRING |
| OTHER                    | BINARY                    | STRING (UTF8)                   | Default for unrecognized OTHER types  |
| STRUCT                   |                           |                                 | NOT SUPPORTED (fails, as in Avro)     |
| REF / REF_CURSOR         |                           |                                 | NOT SUPPORTED (fails, as in Avro)     |
| DATALINK                 |                           |                                 | NOT SUPPORTED (fails, as in Avro)     |
| all other Java SQL types | BINARY                    | STRING (UTF8)                   |                                       |

#### Native Parquet types (`--parquetNativeTypes`)

By default DATE, TIME and DECIMAL columns use the same encodings as the Avro export. With
`--parquetNativeTypes` (default `false`; requires `--useLogicalTypes`, otherwise the job fails
with `IllegalArgumentException`), they use native Parquet types instead:

| **Java SQL type**        | **Parquet type**                                                     |
|--------------------------|----------------------------------------------------------------------|
| DATE                     | `int32 (DATE)`: days since 1970-01-01                                |
| TIME, TIME_WITH_TIMEZONE | `int32 (TIME(MILLIS,true))`; `int64 (TIME(MICROS,true))` with `--useTimestampMicros` |
| DECIMAL / NUMERIC        | `fixed_len_byte_array(n) (DECIMAL(precision,scale))` when the JDBC precision is 1..38 and 0 <= scale <= precision; `n` is the smallest length holding the precision (e.g. 3 bytes for precision 5, 16 for 38) |

Time values are UTC (`isAdjustedToUTC=true`, like the timestamps), read from the column the same
way as timestamps. DECIMAL values are rescaled to the column scale; a value with more fractional
digits than the scale or more digits than the precision fails the export with a clear error
instead of being rounded or truncated. DECIMAL always uses `fixed_len_byte_array` (valid for any
precision in the Parquet spec) rather than `int32`/`int64` for small precisions, because
parquet-avro maps `int32`/`int64` DECIMAL to a plain Avro `int`/`long`, which would drop the scale
from the footer Avro schema.

DECIMAL/NUMERIC columns without a usable precision keep the string encoding: e.g. PostgreSQL's
unconstrained `NUMERIC` reports precision 0 (131089 with older drivers), H2's unconstrained
`NUMERIC` reports 100000, and a precision above 38 or a negative scale also falls back.

The footer Avro schema (`AvroSchemaConverter`) maps these to Avro `int`/`date`,
`int`/`time-millis` or `long`/`time-micros`, and `fixed`/`decimal`.

#### Array element type mapping

Array element types are inferred from the column type name. For PostgreSQL, the type name is prefixed with underscore (e.g. `_int4`). For H2 and other databases, the type name is parsed from patterns like `INTEGER ARRAY`.

| **Column type name**     | **Element Parquet type** |
|--------------------------|--------------------------|
| `_int`, `_int4`, `_int2` | INT32                    |
| `_int8`                  | INT64                    |
| `_float4`                | FLOAT                    |
| `_float8`                | DOUBLE                   |
| `_bool`                  | BOOLEAN                  |
| `_bytea`                 | BINARY                   |
| `_text`, `_varchar`, `_bpchar`, `_char`, `_name`, `_uuid` | BINARY (STRING) |
| `INTEGER ARRAY`, `SMALLINT ARRAY` | INT32           |
| `BIGINT ARRAY`           | INT64                    |
| `DOUBLE PRECISION ARRAY` | DOUBLE                   |
| `BOOLEAN ARRAY`          | BOOLEAN                  |
| `VARCHAR ARRAY`, `CHARACTER VARYING ARRAY`, `UUID ARRAY` | BINARY (STRING) |

Any other element type (e.g. `_timestamp`, `_date`, `_numeric`, `_json`, `TIMESTAMP ARRAY`) fails
schema generation, before any output is written, with an error naming the column and the element
type. Use `--arrayMode=bytes` to export such columns as bytes.

Multidimensional arrays (e.g. `int4[][]`, `bytea[][]`) are not supported with typed arrays: the
JDBC metadata does not expose the number of dimensions, so the export fails on the first such
value with an error naming the column. Use `--arrayMode=bytes` for them.

As in the Avro export, typed list elements are `required` unless `--nullableArrayItems` is set,
which makes them `optional`. Without the flag, a NULL array item fails the export with `Array item
is null in column '<column>', use --nullableArrayItems` before any part of that value is written.
`--nullableArrayItems` is ignored with `--arrayMode=bytes`.

#### Footer Avro schema (`parquet.avro.schema`)

The Avro schema stored in the `parquet.avro.schema` footer key, and saved as `_AVRO_SCHEMA.avsc`
(identical content), is derived from the Parquet schema actually used for writing, including a
schema given with `--parquetSchemaFilePath`. It therefore always describes the written columns,
and `AvroParquetReader` (Scio, Beam `ParquetIO`, Spark with parquet-avro) can decode the files
with it. The conversion is parquet-avro's `AvroSchemaConverter`, configured with
`parquet.avro.add-list-element-records=false` so lists map directly to Avro arrays:

| **Parquet type**                               | **Avro type**                                    |
|------------------------------------------------|--------------------------------------------------|
| `optional` field                               | `["null", T]` with default `null`                |
| `required` field                               | `T`                                              |
| BOOLEAN / INT32 / INT64 / FLOAT / DOUBLE       | boolean / int / long / float / double            |
| INT64 TIMESTAMP(MILLIS\|MICROS, UTC)           | long `timestamp-millis` \| `timestamp-micros`    |
| INT64 TIMESTAMP(..., not UTC)                  | long `local-timestamp-millis` \| `-micros`       |
| INT32 DATE / TIME(MILLIS), INT64 TIME(MICROS)  | `date` / `time-millis` / `time-micros`           |
| BINARY STRING / ENUM / JSON                    | string                                           |
| BINARY (no annotation) / DECIMAL               | bytes / bytes `decimal`                          |
| FIXED_LEN_BYTE_ARRAY(16) UUID                  | string `uuid` (decoded to the canonical UUID string) |
| FIXED_LEN_BYTE_ARRAY(n) / DECIMAL              | fixed(n) / fixed `decimal`                       |
| LIST                                           | array (no wrapper record); items are `T` for `required` elements, `["null", T]` for `optional` ones (`--nullableArrayItems`) |
| MAP with STRING keys                           | map                                              |
| group                                          | nested record                                    |

The record is named after the Parquet message (`--avroSchemaName` or the table name), uses
`--avroSchemaNamespace` and `--avroDoc` (default `Generate schema from JDBC ResultSet from
<connection URL>`). Unlike dbeam-core's Avro export, it carries no `tableName`/`connectionUrl`
record properties and no per-field docs or properties. A schema file with types that have no Avro equivalent (e.g. INT96) or names that
are not valid Avro names is exported without the footer key and `_AVRO_SCHEMA.avsc`.

#### Compression codec (`--parquetCodec`)

`--parquetCodec` selects the Parquet compression codec and defaults to `snappy`. Supported
values: `snappy`, `gzip`, `zstd`, `zstd1` .. `zstd22` (Zstandard with that compression level,
set via `parquet.compression.codec.zstd.level`; plain `zstd` uses parquet-hadoop's default level
3), `lz4` (LZ4_RAW), `none` / `uncompressed`. Other compression levels (e.g. for gzip) are not
supported. An invalid value fails at the start of the export, before any metadata file is
written, with `IllegalArgumentException` listing the supported values.

`--avroCodec` is ignored by Parquet exports; a non-default value logs a warning. Since
`--avroCodec` has a default (`deflate6`), passing exactly `--avroCodec=deflate6` cannot be
detected and logs nothing.

#### Input schema file (`--parquetSchemaFilePath`)

`--avroSchemaFilePath` is not supported by Parquet jobs: passing it fails immediately with an
`IllegalArgumentException` (exit code 43) before any query runs or output is written.

The Parquet writers always take column types from the JDBC result set and write fields by
position, so an input schema file can only rename fields and adjust annotations. Before the
export starts, the file is checked against the schema derived from JDBC metadata (the table
above, using the job's `--useTimestampMicros` value) with a single `LIMIT 1` query, and the job
fails with a `FailedValidationException` listing every offending column if:

- the number of fields differs (at the top level or inside a `LIST` group),
- a field changes between primitive and group,
- a primitive type (`int32`, `int64`, `binary`, ...) or `fixed_len_byte_array` length differs,
- a repetition differs (all derived fields are `optional`, except typed list elements, which are
  `required` without `--nullableArrayItems` and `optional` with it),
- a group's annotation differs (`LIST` groups must stay `LIST`),
- a logical type annotation is not one of the allowed ones below.

Allowed annotations on a primitive field:

| **Derived field**                                | **Allowed annotations in the schema file**          |
|--------------------------------------------------|-----------------------------------------------------|
| `binary (STRING)`                                | none, `STRING`, `ENUM`, `JSON`                      |
| `binary` (bytes)                                 | none                                                |
| `int32`                                          | none, `INTEGER(32,true)`                            |
| `int64` from BIGINT                              | none, `INTEGER(64,true)`                            |
| `int64` from TIMESTAMP/DATE/TIME                 | none (written in the flag's unit); `TIMESTAMP(MICROS,true)` with `--useTimestampMicros`, otherwise `TIMESTAMP(MILLIS,true)` (with or without `--useLogicalTypes`) |
| `boolean`, `float`, `double`                     | none                                                |
| PostgreSQL `uuid`                                | `binary (STRING)`, `binary`, or `fixed_len_byte_array(16) (UUID)` (the writer follows the schema file) |
| DATE / TIME / DECIMAL with `--parquetNativeTypes` | none, or the exact native annotation (`DATE`, `TIME(MILLIS\|MICROS,true)`, `DECIMAL(p,s)`) on the native physical type |

Everything else (e.g. `DECIMAL`, `DATE` or `TIME` without `--parquetNativeTypes`, a TIMESTAMP unit that disagrees with
`--useTimestampMicros`, `TIMESTAMP(NANOS,...)`, non-UTC timestamps, narrower `INTEGER` widths, or
`STRING` on raw bytes) is rejected because it would change how the written bytes are read.
