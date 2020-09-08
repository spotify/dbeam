
-- This file contatins psql views with complex types to validate and troubleshoot dbeam
-- import with:
-- psql -f ddl.sql postgres

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Semi realistic table
DROP TABLE IF EXISTS demo_table;
CREATE UNLOGGED TABLE demo_table
AS
SELECT
  n::bigint AS row_number,
  (trunc(random() * 3)::integer > 0)::boolean AS bool_field,
  replace(uuid_generate_v4()::text, '-', '') AS hexid1,
  timestamp '2010-01-01 00:00:00' +
    random() * (timestamp '2010-01-01 00:00:00' -
      timestamp '2020-01-01 00:00:00') AS timestamp1,
  (trunc(random() * 10)::integer + 1) AS tag_field_id,
  'const' AS flag1,
  array_to_string(array
    (SELECT substr('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', trunc(random() * 62)::integer + 1, 1)
    FROM   generate_series(1, 12)), '') AS random_str2,
  (ARRAY[NULL, 1.99, 5.99, 99.99, 155.98]::numeric[]
  )[trunc(random() * 5)::integer + 1] AS numeric_field,
  timestamp '2010-01-01 00:00:00' +
    random() * (timestamp '2010-01-01 00:00:00' -
      timestamp '2020-01-01 00:00:00') AS timestamp2,
  E'\\000'::bytea AS bytes_field,
  ARRAY['foo', 'bar']::text[] AS arr1
FROM
  generate_series(1,1000000) a(n)
  ;
ANALYZE demo_table;
EXPLAIN ANALYZE SELECT * FROM demo_table;

CREATE SCHEMA IF NOT EXISTS test_schema;
DROP TABLE IF EXISTS test_schema.schema_demo_table;
CREATE UNLOGGED TABLE test_schema.schema_demo_table
AS
SELECT 1::integer foo, 'bar'::text bar;
EXPLAIN ANALYZE SELECT * FROM test_schema.schema_demo_table;
