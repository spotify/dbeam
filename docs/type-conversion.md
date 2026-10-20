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

`--useTimestampMicros` alone decides the unit, also with `--avroSchemaFilePath` (whose schema is
used verbatim as the writer schema). The job fails fast with `FailedValidationException` (exit
code 50), before writing any output, if the schema
file has a field (including inside unions, arrays, maps and nested records) with logical type
`timestamp-millis` while the flag is set, `timestamp-micros` while it is not, or any
`local-timestamp-*` / `timestamp-nanos`. Plain `long` fields are accepted and hold the flag's unit.

More generally, values are binary-encoded by field position from the JDBC column types (the
table below), so before the export a single `LIMIT 1` query derives the expected schema and the
schema file must match it except for record/field names, docs, namespaces and properties. The
job fails with `FailedValidationException` listing every offending field if:

- the number of fields differs,
- a field is not a `["null", T]` union (null first; values are written with union index 0 for
  NULL and 1 otherwise),
- `T` has a different Avro type than the one written (e.g. `bytes`/`decimal` or `fixed` for a
  DECIMAL column, which is written as `string`; `int` for a TIMESTAMP column, written as `long`),
- array items have a different type, or their nullability does not follow `--nullableArrayItems`
  (`[T, "null"]` with the flag, `T` without),
- a `logicalType` is neither absent nor the one dbeam generates for that column with
  `--useLogicalTypes` (`timestamp-millis`/`timestamp-micros` per `--useTimestampMicros`, `uuid`
  for PostgreSQL uuid); e.g. `date`, `time-*`, `local-timestamp-*` or `decimal` are rejected.

With `--arrayMode=typed_first_row` and no row (or a NULL first value), the array item type
cannot be derived and is taken from the schema file.

To represent nullable columns, unions with the Avro NULL type are used. Field names are the
column names with characters outside `[A-Za-z0-9_]` replaced by `_`; if two columns end up with
the same field name (e.g. `a-b` and `a_b`), the export fails listing the colliding columns.

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
