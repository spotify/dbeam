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
fails fast, so the Parquet schema and the footer Avro schema cannot disagree.

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

#### Codec mapping

When using `JdbcParquetJob`, the `--avroCodec` parameter is mapped to Parquet compression codecs:

| **--avroCodec value** | **Parquet codec** |
|-----------------------|-------------------|
| snappy                | SNAPPY            |
| deflate1 .. deflate9  | GZIP              |
| zstandard1 .. zstandard9 | ZSTD           |
