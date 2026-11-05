
-- This file contains psql views with complex types to validate and troubleshoot dbeam
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
  'const'::text AS flag1,
  'const'::varchar AS flag2,
  array_to_string(array
    (SELECT substr('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', trunc(random() * 62)::integer + 1, 1)
    FROM   generate_series(1, 12)), '') AS random_str2,
  (ARRAY[NULL, 1.99, 5.99, 99.99, 155.98]::numeric[]
  )[trunc(random() * 5)::integer + 1] AS numeric_field,
  timestamp '2010-01-01 00:00:00' +
    random() * (timestamp '2010-01-01 00:00:00' -
      timestamp '2020-01-01 00:00:00') AS timestamp2,
  gen_random_uuid()::uuid as uuid1,
  E'\\000'::bytea AS bytes_field,
  ARRAY['foo', 'bar']::text[] AS arr1,
  ARRAY[42, 777]::integer[] AS arr2,
  ARRAY[21474836471, 21474836479]::bigint[] AS arr3,
-- TODO: ADD normal numeric support
--   ARRAY[1.99, 5.99]::numeric[] AS arr4,
  ARRAY['foo', 'bar']::varchar(12)[] AS arr5,
  ARRAY[gen_random_uuid(), gen_random_uuid()]::uuid[] AS arr6
FROM
  generate_series(1,1000000) a(n)
;
ANALYZE demo_table;
EXPLAIN ANALYZE SELECT * FROM demo_table;

-- Table for timestamp-micros precision testing
DROP TABLE IF EXISTS timestamp_micros_test;
CREATE TABLE timestamp_micros_test (
  id serial PRIMARY KEY,
  name text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO timestamp_micros_test (name, created_at, updated_at)
VALUES
  ('row1', '2026-08-20 19:06:01.879123+00',
          '2026-08-20 19:06:02.123456+00'),
  ('row2', '2026-08-20 19:06:01.879456+00',
          '2026-08-20 19:06:02.654321+00'),
  ('row3', '2026-08-20 19:06:01.879000+00',
          '2026-08-20 19:06:02.000000+00');

-- typed_postgres arrays whose elements pgjdbc may not return as String (#1 in review):
-- json[]/jsonb[] (PGobject?) and char(n)[] (bpchar), next to bool/float arrays
DROP TABLE IF EXISTS typed_postgres_array_test;
CREATE TABLE typed_postgres_array_test (
  id integer PRIMARY KEY,
  json_arr json[],
  jsonb_arr jsonb[],
  bpchar_arr char(3)[],
  bool_arr boolean[],
  float4_arr real[],
  float8_arr double precision[]
);
INSERT INTO typed_postgres_array_test VALUES
  (1,
   ARRAY['{"a": 1}', '[1, 2]']::json[],
   ARRAY['{"b": true}', '"x"']::jsonb[],
   ARRAY['ab', 'xyz']::char(3)[],
   ARRAY[true, false],
   ARRAY[1.5, -2.25]::real[],
   ARRAY[0.1, 1e300]::double precision[]),
  (2, NULL, NULL, NULL, NULL, NULL, NULL);
