# Output Avro schema

## Generated Avro schema

`dbeam` generates an Avro schema using an SQL query result.
There are a number of settings to configure a few the resulting schema. 
 * `--avroDoc=<String>`
 * `--avroSchemaName=<String>`
 * `--avroSchemaNamespace=<String>`
 * `--useAvroLogicalTypes=<Boolean>`

However, there is no way to supply your own fields' `doc` properties.
They will be auto-generated.

## Custom provided schema
A user can provide own Avro schema using a parameter
 * `--avroSchemaFilePath`.

E.g. `--avroSchemaFilePath=/temp/my_record.avsc`

The main purpose of using this approach is to supply own values for Avro record fields' `doc` attribute for a generated Avro schema used in output Avro files.

### Fields look-up
Avro record fields in the provided schema are looked up using field `name`, so it is important that the fields' names are the same as in the generated schema.
Thus, it is recommended to copy the generated schema, update `doc` fields and use it as `--avroSchemaFilePath` input.   

### A generated Avro schema example
```
{
  "type" : "record",
  "name" : "table_numbers",
  "namespace" : "dbeam_generated",
  "doc" : "Generate schema from JDBC ResultSet from jdbc:mysql://localhost/dev_db",
  "fields" : [ {
    "name" : "id",
    "type" : [ "null", "string" ],
    "doc" : "From sqlType -5 BIGINT",
    "default" : null,
    "typeName" : "BIGINT",
    "sqlCode" : "-5",
    "columnName" : "id"
  }, {
    "name" : "seq_num",
    "type" : [ "null", "int" ],
    "doc" : "From sqlType 4 INTEGER",
    "default" : null,
    "typeName" : "INTEGER",
    "sqlCode" : "4",
    "columnName" : "seq_num"
  } ],
  "connectionUrl" : "jdbc:mysql://localhost/dev_db",
  "tableName" : "table_numbers"
}
```
### A user-provided schema example
``` 
{
  "type" : "record",
  "name" : "Your own record name",
  "namespace" : "You own namespace",
  "doc" : "Your own description",
  "fields" : [
  {
    "name" : "id",
    "type" : [ "null", "string" ],
    "default" : null,
    "doc" : "Your own field description here. #DATA_LEVEL7"
  },
  {
    "name" : "seq_num",
    "type" : [ "null", "int" ],
    "default" : null,
    "doc" : "Your own field description here. #DATA_LEVEL8"
  } 
  ]
}
```
