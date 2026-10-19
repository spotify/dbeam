### Type Conversion Details: Java SQL ==> Parquet

Java SQL types (java.sql.Types.*) are converted to Parquet types according to the following table.
When applicable and `--useAvroLogicalTypes` parameter is set to `true`, Parquet logical types are used.

All columns are represented as `optional` (nullable) fields in the Parquet schema.

| **Java SQL type**        | **Parquet physical type** | **Parquet logical type**        | **Comments**                          |
|--------------------------|---------------------------|---------------------------------|---------------------------------------|
| BIGINT                   | INT64                     |                                 |                                       |
| INTEGER                  | INT32                     |                                 | INT64 if column class is java.lang.Long (e.g. MySQL unsigned int) |
| SMALLINT                 | INT32                     |                                 |                                       |
| TINYINT                  | INT32                     |                                 |                                       |
| TIMESTAMP                | INT64                     | TIMESTAMP(MILLIS, isAdjustedToUTC=true) | Logical type only with `--useAvroLogicalTypes` |
| DATE                     | INT64                     | TIMESTAMP(MILLIS, isAdjustedToUTC=true) | Logical type only with `--useAvroLogicalTypes` |
| TIME                     | INT64                     | TIMESTAMP(MILLIS, isAdjustedToUTC=true) | Logical type only with `--useAvroLogicalTypes` |
| TIME_WITH_TIMEZONE       | INT64                     | TIMESTAMP(MILLIS, isAdjustedToUTC=true) | Logical type only with `--useAvroLogicalTypes` |
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
| OTHER (uuid)             | FIXED_LEN_BYTE_ARRAY(16)  | UUID                            | Only with `--useAvroLogicalTypes`; otherwise STRING |
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

#### Input schema file (`--parquetSchemaFilePath`)

The Parquet writers always take column types from the JDBC result set and write fields by
position, so an input schema file can only rename fields and adjust annotations. Before the
export starts, the file is checked against the schema derived from JDBC metadata (the table
above) and the job fails with a `FailedValidationException` listing every offending column if:

- the number of fields differs (at the top level or inside a `LIST` group),
- a field changes between primitive and group,
- a primitive type (`int32`, `int64`, `binary`, ...) or `fixed_len_byte_array` length differs,
- a repetition differs (all derived fields are `optional`; `required` is rejected),
- a group's annotation differs (`LIST` groups must stay `LIST`),
- a logical type annotation is not one of the allowed ones below.

Allowed annotations on a primitive field:

| **Derived field**                                | **Allowed annotations in the schema file**          |
|--------------------------------------------------|-----------------------------------------------------|
| `binary (STRING)`                                | none, `STRING`, `ENUM`, `JSON`                      |
| `binary` (bytes)                                 | none                                                |
| `int32`                                          | none, `INTEGER(32,true)`                            |
| `int64` from BIGINT                              | none, `INTEGER(64,true)`                            |
| `int64` from TIMESTAMP/DATE/TIME                 | none, `TIMESTAMP(MILLIS,true)` (with or without `--useAvroLogicalTypes`) |
| `boolean`, `float`, `double`                     | none                                                |
| PostgreSQL `uuid`                                | `binary (STRING)`, `binary`, or `fixed_len_byte_array(16) (UUID)` (the writer follows the schema file) |

Everything else (e.g. `DECIMAL`, `DATE`, `TIMESTAMP(MICROS,...)`, narrower `INTEGER` widths, or
`STRING` on raw bytes) is rejected because it would change how the written bytes are read.
