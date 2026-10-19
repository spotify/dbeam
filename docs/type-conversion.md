### Type Conversion Details: Java SQL ==> Avro 

Java SQL types (java.sql.Types.*) are converted to Avro types according to the following table.
When applicable and `--useLogicalTypes` parameter is set to `true`, Avro logical types are used.
`--useAvroLogicalTypes` is a deprecated alias of `--useLogicalTypes` and is still supported;
if both are set they must agree.

When `--useTimestampMicros` is also set (it requires `--useLogicalTypes`), TIMESTAMP, DATE, TIME
and TIME_WITH_TIMEZONE columns use `logicalType: timestamp-micros` instead of `timestamp-millis`,
and values are written as epoch microseconds, preserving the sub-millisecond part of
`java.sql.Timestamp` (e.g. PostgreSQL TIMESTAMP/TIMESTAMPTZ, which store microseconds).
Without `--useTimestampMicros`, values are epoch milliseconds and sub-millisecond precision is
truncated. Without `--useLogicalTypes`, these columns are a plain `long` with no logical type.

To represent nullable columns, unions with the Avro NULL type are used.

| **Java SQL type**        | **Avro type**   | **Avro schema annotations** | **Comments**                          |
|--------------------------|-----------------|-----------------------------|---------------------------------------|
| BIGINT                   | long / string   |                             | Depends on a SQL column precision     |
| INTEGER                  | int             |                             |                                       |
| SMALLINT                 | int             |                             |                                       |
| TINYINT                  | int             |                             |                                       |
| TIMESTAMP                | long            | logicalType: timestamp-millis | timestamp-micros with `--useTimestampMicros` |
| DATE                     | long            | logicalType: timestamp-millis | timestamp-micros with `--useTimestampMicros` |
| TIME                     | long            | logicalType: timestamp-millis | timestamp-micros with `--useTimestampMicros` |
| TIME_WITH_TIMEZONE       | long            | logicalType: timestamp-millis | timestamp-micros with `--useTimestampMicros` |
| BOOLEAN                  | boolean         |                             |                                       |
| BIT                      | boolean / bytes |                             | Depends on a SQL column precision     |
| BINARY                   | bytes           |                             |                                       |
| BINARY                   | bytes           |                             |                                       |
| VARBINARY                | bytes           |                             |                                       |
| LONGVARBINARY            | bytes           |                             |                                       |
| ARRAY                    | array           |                             | Propagates an array item type as well |
| OTHER                    | string          | logicalType: uuid           |                                       |
| BLOB                     | bytes           |                             |                                       |
| DOUBLE                   | double          |                             |                                       |
| FLOAT                    | float           |                             |                                       |
| REAL                     | float           |                             |                                       |
| VARCHAR                  | string          |                             |                                       |
| CHAR                     | string          |                             |                                       |
| CLOB                     | string          |                             |                                       |
| LONGNVARCHAR             | string          |                             |                                       |
| LONGVARCHAR              | string          |                             |                                       |
| NCHAR                    | string          |                             |                                       |
| STRUCT                   |                 |                             | NOT SUPPORTED                         |
| REF                      |                 |                             | NOT SUPPORTED                         |
| REF_CURSOR               |                 |                             | NOT SUPPORTED                         |
| DATALINK                 |                 |                             | NOT SUPPORTED                         |
| all other Java SQL types | string          |                             |                                       |
