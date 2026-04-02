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
| ARRAY                    | BINARY                    |                                 | Serialized as string representation   |
| DOUBLE                   | DOUBLE                    |                                 |                                       |
| FLOAT                    | FLOAT                     |                                 |                                       |
| REAL                     | FLOAT                     |                                 |                                       |
| VARCHAR                  | BINARY                    | STRING (UTF8)                   |                                       |
| CHAR                     | BINARY                    | STRING (UTF8)                   |                                       |
| CLOB                     | BINARY                    | STRING (UTF8)                   |                                       |
| LONGNVARCHAR             | BINARY                    | STRING (UTF8)                   |                                       |
| LONGVARCHAR              | BINARY                    | STRING (UTF8)                   |                                       |
| NCHAR                    | BINARY                    | STRING (UTF8)                   |                                       |
| OTHER (uuid)             | FIXED_LEN_BYTE_ARRAY(16)  | UUID                            | Only with `--useAvroLogicalTypes`; otherwise STRING |
| OTHER                    | BINARY                    | STRING (UTF8)                   | Default for unrecognized OTHER types  |
| all other Java SQL types | BINARY                    | STRING (UTF8)                   |                                       |

#### Codec mapping

When using `JdbcParquetJob`, the `--avroCodec` parameter is mapped to Parquet compression codecs:

| **--avroCodec value** | **Parquet codec** |
|-----------------------|-------------------|
| snappy                | SNAPPY            |
| deflate1 .. deflate9  | GZIP              |
| zstandard1 .. zstandard9 | ZSTD           |
