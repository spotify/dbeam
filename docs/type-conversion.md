### Type Conversion Details: Java SQL ==> Avro 

Java SQL types (java.sql.Types.*) are converted to Avro types according to the following table.
When applicable and `--useAvroLogicalTypes` parameter is set to `true`, Avro logical types are used.

To represent nullable columns, unions with the Avro NULL type are used.

| **Java SQL type**        | **Avro type**   | **Avro schema annotations** | **Comments**                          |
|--------------------------|-----------------|-----------------------------|---------------------------------------|
| BIGINT                   | long / string   |                             | Depends on a SQL column precision     |
| INTEGER                  | int             |                             |                                       |
| SMALLINT                 | int             |                             |                                       |
| TINYINT                  | int             |                             |                                       |
| TIMESTAMP                | long            | logicalType: time-millis    |                                       |
| DATE                     | long            | logicalType: time-millis    |                                       |
| TIME                     | long            | logicalType: time-millis    |                                       |
| TIME_WITH_TIMEZONE       | long            | logicalType: time-millis    |                                       |
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
