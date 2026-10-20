### Type Conversion Details: Java SQL ==> Parquet

Java SQL types (java.sql.Types.*) are converted to Parquet types according to the following table.
When applicable and `--useLogicalTypes` parameter is set to `true`, Parquet logical types are used.
`--useAvroLogicalTypes` is a deprecated alias of `--useLogicalTypes` and is still supported.

All columns are represented as `optional` (nullable) fields in the Parquet schema.

When `--useTimestampMicros` is also set (it requires `--useLogicalTypes`), TIMESTAMP, DATE,
TIME and TIME_WITH_TIMEZONE columns use `TIMESTAMP(MICROS, isAdjustedToUTC=true)` and values are
written as epoch microseconds, keeping the sub-millisecond part of `java.sql.Timestamp`. The Avro
schema stored in the `parquet.avro.schema` footer key then uses `timestamp-micros` to match.
When `--parquetSchemaFilePath` is used, every TIMESTAMP field in the schema file must use the unit
selected by `--useTimestampMicros` (MICROS with the flag, MILLIS without it); otherwise the job
fails fast instead of silently exporting a different unit.

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
| DECIMAL / NUMERIC        | BINARY                    | STRING (UTF8)                   | Exported as string representation; no fixed-point Parquet DECIMAL type |
| OTHER (uuid)             | FIXED_LEN_BYTE_ARRAY(16)  | UUID                            | Only with `--useLogicalTypes`; otherwise STRING |
| OTHER                    | BINARY                    | STRING (UTF8)                   | Default for unrecognized OTHER types  |
| all other Java SQL types | BINARY                    | STRING (UTF8)                   |                                       |

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
| `_text`, `_varchar`, `_uuid`, others | BINARY (STRING) |
| `INTEGER ARRAY`          | INT32                    |
| `BIGINT ARRAY`           | INT64                    |
| `VARCHAR ARRAY`, others  | BINARY (STRING)          |

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
| LIST                                           | array; items are `["null", T]` (no wrapper record) because dbeam always writes optional list elements |
| MAP with STRING keys                           | map                                              |
| group                                          | nested record                                    |

The record is named after the Parquet message (`--avroSchemaName` or the table name), uses
`--avroSchemaNamespace` and `--avroDoc` (default `Generate schema from JDBC ResultSet from
<connection URL>`). Unlike dbeam-core's Avro export, it carries no `tableName`/`connectionUrl`
record properties and no per-field docs or properties. `--nullableArrayItems` has no effect on
Parquet exports. A schema file with types that have no Avro equivalent (e.g. INT96) or names that
are not valid Avro names is exported without the footer key and `_AVRO_SCHEMA.avsc`.

#### Codec mapping

When using `JdbcParquetJob`, the `--avroCodec` parameter is mapped to Parquet compression codecs:

| **--avroCodec value** | **Parquet codec** |
|-----------------------|-------------------|
| snappy                | SNAPPY            |
| deflate1 .. deflate9  | GZIP              |
| zstandard1 .. zstandard9 | ZSTD           |
