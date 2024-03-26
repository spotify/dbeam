
DBeam
=======

[![Github Actions Build Status](https://github.com/spotify/dbeam/actions/workflows/maven.yml/badge.svg)](https://github.com/spotify/dbeam/actions/workflows/maven.yml)
[![codecov.io](https://codecov.io/github/spotify/dbeam/coverage.svg?branch=master)](https://codecov.io/github/spotify/dbeam?branch=master)
[![Apache Licensed](https://img.shields.io/github/license/spotify/dbeam.svg)](https://opensource.org/licenses/Apache-2.0)
[![GitHub tag](https://img.shields.io/github/tag/spotify/dbeam)](https://github.com/spotify/dbeam/releases/?include_prereleases&sort=semver)
[![Maven Central](https://img.shields.io/maven-central/v/com.spotify/dbeam-core.svg)](https://maven-badges.herokuapp.com/maven-central/com.spotify/dbeam-core)

A connector tool to extract data from SQL databases and import into [GCS](https://cloud.google.com/storage/) using [Apache Beam](https://beam.apache.org/).

This tool is runnable locally, or on any other backend supported by Apache Beam, e.g. [Cloud Dataflow](https://cloud.google.com/dataflow/).

**DEVELOPMENT STATUS: Mature, maintained and used in production since August 2017. No major features or development planned.**

## Overview

DBeam is tool based that reads all the data from single SQL database table,
converts the data into [Avro](https://avro.apache.org/) and stores it into
appointed location, usually in GCS.
It runs as a single threaded [Apache Beam](https://beam.apache.org/) pipeline.

DBeam requires the database credentials, the database table name to read, and the output location
to store the extracted data into. DBeam first makes a single select into the target table with
limit one to infer the table schema. After the schema is created the job will be launched which
simply streams the table contents via JDBC into target location as Avro.

[Generated Avro Schema Type Conversion Details](docs/type-conversion.md)


## dbeam-core package features

- Supports both PostgreSQL and MySQL JDBC connectors
- Supports [Google CloudSQL](https://cloud.google.com/sql/) managed databases
- Currently output only to Avro format
- Reads database from an external password file (`--passwordFile`) or an external [KMS](https://cloud.google.com/kms/) encrypted password file (`--passwordFileKmsEncrypted`)
- Can filter only records of the current day with the `--partitionColumn` parameter
- Check and fail on too old partition dates. Snapshot dumps are not filtered by a given date/partition, when running for a too old partition, the job fails to avoid new data in old partitions. (can be disabled with `--skipPartitionCheck`)
- Implemented as [Apache Beam SDK](https://beam.apache.org/) pipeline, supporting any of its [runners](https://beam.apache.org/documentation/runners/capability-matrix/) (tested with `DirectRunner` and `DataflowRunner`)

### DBeam export parameters

```
com.spotify.dbeam.options.DBeamPipelineOptions:

  --connectionUrl=<String>
    The JDBC connection url to perform the export.
  --password=<String>
    Plaintext password used by JDBC connection.
  --passwordFile=<String>
    A path to a file containing the database password.
  --passwordFileKmsEncrypted=<String>
    A path to a file containing the database password, KMS encrypted and base64
    encoded.
  --sqlFile=<String>
    A path to a file containing a SQL query (used instead of --table parameter).
  --table=<String>
    The database table to query and perform the export.
  --username=<String>
    Default: dbeam-extractor
    The database user name used by JDBC to authenticate.

com.spotify.dbeam.options.OutputOptions:

  --output=<String>
    The path for storing the output.
  --dataOnly=<Boolean>
    Default: false
    Store only the data files in output folder, skip queries, metrics and
    metadata files.

com.spotify.dbeam.options.JdbcExportPipelineOptions:
    Configures the DBeam SQL export

  --avroCodec=<String>
    Default: deflate6
    Avro codec (e.g. deflate6, deflate9, snappy).
  --avroDoc=<String>
    The top-level record doc string of the generated avro schema.
  --avroSchemaFilePath=<String>
    Path to file with a target AVRO schema.
  --avroSchemaName=<String>
    The name of the generated avro schema, the table name by default.
  --avroSchemaNamespace=<String>
    Default: dbeam_generated
    The namespace of the generated avro schema.
  --exportTimeout=<String>
    Default: P7D
    Export timeout, after this duration the job is cancelled and the export
    terminated.
  --fetchSize=<Integer>
    Default: 10000
    Configures JDBC Statement fetch size.
  --limit=<Long>
    Limit the output number of rows, indefinite by default.
  --minPartitionPeriod=<String>
    The minimum partition required for the job not to fail (when partition
    column is not specified),by default `now() - 2*partitionPeriod`.
  --minRows=<Long>
    Default: -1
    Check that the output has at least this minimum number of rows. Otherwise
    fail the job.
  --partition=<String>
    The date/timestamp of the current partition.
  --partitionColumn=<String>
    The name of a date/timestamp column to filter data based on current
    partition.
  --partitionPeriod=<String>
    The period frequency which the export runs, used to filter based on current
    partition and also to check if exports are running for too old partitions.
  --preCommand=<List>
    SQL commands to be executed before query.
  --queryParallelism=<Integer>
    Max number of queries to run in parallel for exports. Single query used if
    nothing specified. Should be used with splitColumn.
  --skipPartitionCheck=<Boolean>
    Default: false
    When partition column is not specified, fails if partition is too old; set
    this flag to ignore this check.
  --splitColumn=<String>
    A long/integer column used to create splits for parallel queries. Should be
    used with queryParallelism.
  --useAvroLogicalTypes=<Boolean>
    Default: false
    Controls whether generated Avro schema will contain logicalTypes or not.
```

#### Input Avro schema file

If provided an input Avro schema file, dbeam will read input schema file and use some of the 
properties when an output Avro schema is created.

#### Following fields will be propagated from input into output schema:

* `record.doc`
* `record.namespace`
* `record.field.doc`


#### DBeam Parallel Mode

This is a pre-alpha feature currently under development and experimentation.

Read queries used by dbeam to extract data generally don't place any locks, and hence multiple read queries
can run in parallel. When running in parallel mode with `--queryParallelism` specified, dbeam looks for
`--splitColumn` argument to find the max and min values in that column. The max and min are then used
as range bounds for generating `queryParallelism` number of queries which are then run in parallel to read data. 
Since the splitColumn is used to calculate the query bounds, and dbeam needs to calculate intermediate
bounds for each query, the type of the column must be long / int. It is assumed that the distribution of values on the `splitColumn` is sufficiently random and sequential. Example if the min and max of the split column is divided equally into query parallelism parts, each part would contain approximately equal number of records. Having skews in this data would result in straggling queries, and hence wont provide much improvement. Having the records sequential would help in having the queries run faster and it would reduce random disk seeks.

Recommended usage:
Beam would run each query generated by DBeam in 1 dedicated vCPU (when running with Dataflow Runner), thus for best performance it is recommended that the total number of vCPU available for a given job should be equal to the `queryParallelism` specified. Hence if `workerMachineType` for Dataflow is `n1-standard-w` and `numWorkers` is `n` then `queryParallelism` `q` should be a multiple of `n*w` and the job would be fastest if `q = n * w`.

For an export of a table running from a dedicated PostgresQL replica, we have seen best performance over vCPU time and wall time when having a `queryParallelism` of 16. Bumping `queryParallelism` further increases the vCPU time without offering much gains on the wall time of the complete export. It is probably good to use `queryParallelism` less than 16 for experimenting.

## Building

Building and testing can be achieved with `mvn`:

```sh
mvn verify
```

In order to create a jar with all dependencies under `./dbeam-core/target/dbeam-core-shaded.jar` run the following:

```sh
mvn clean package -Ppack
```

## Usage examples

Using Java from the command line:

```sh
java -cp ./dbeam-core/target/dbeam-core-shaded.jar \
  com.spotify.dbeam.jobs.JdbcAvroJob \
  --output=gs://my-testing-bucket-name/ \
  --username=my_database_username \
  --password=secret \
  --connectionUrl=jdbc:postgresql://some.database.uri.example.org:5432/my_database \
  --table=my_table
```

For CloudSQL:

```sh
java -cp ./dbeam-core/target/dbeam-core-shaded.jar \
  com.spotify.dbeam.jobs.JdbcAvroJob \
  --output=gs://my-testing-bucket-name/ \
  --username=my_database_username \
  --password=secret \
  --connectionUrl=jdbc:postgresql://google/database?socketFactory=com.google.cloud.sql.postgres.SocketFactory&socketFactoryArg=project:region:cloudsql-instance \
  --table=my_table
```

- When using MySQL: `--connectionUrl=jdbc:mysql://google/database?socketFactory=com.google.cloud.sql.mysql.SocketFactory&cloudSqlInstance=project:region:cloudsql-instance&useCursorFetch=true`
- Note `?useCursorFetch=true` is important for MySQL, to avoid early fetching all rows, more details on [MySQL docs](https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-implementation-notes.html).
- More details can be found at [CloudSQL JDBC SocketFactory](https://github.com/GoogleCloudPlatform/cloud-sql-jdbc-socket-factory)

To run a cheap data extraction, as a way to validate, one can add `--limit=10 --skipPartitionCheck` parameters. It will run the queries, generate the schemas and export only 10 records, which should be done in a few seconds.

### Password configuration

Database password can be configured by simply passing `--password=writepasswordhere`, `--passwordFile=/path/to/file/containing/password` or `--passwordFile=gs://gcs-bucket/path/to/file/containing/password`.

A more robust configuration is to point to a [Google KMS](https://cloud.google.com/kms/) encrypted file.
DBeam will try to decrypt using KMS if the file ends with `.encrypted` (e.g. `--passwordFileKmsEncrypted=gs://gcs-bucket/path/to/db-password.encrypted`).

The file should contain a base64 encoded encrypted content.
It can be generated using [`gcloud`](https://cloud.google.com/sdk/gcloud/) like the following:

```sh
echo -n "super_secret_password" \
  | gcloud kms encrypt \
      --location "global" \
      --keyring "dbeam" \
      --key "default" \
      --project "mygcpproject" \
      --plaintext-file - \
      --ciphertext-file - \
  | base64 \
  | gsutil cp - gs://gcs-bucket/path/to/db-password.encrypted
```

KMS location, keyring, and key can be configured via Java Properties, defaults are:


```sh
java \
  -DKMS_KEYRING=dbeam \
  -DKMS_KEY=default \
  -DKMS_LOCATION=global \
  -DKMS_PROJECT=default_gcp_project \
  -cp ./dbeam-core/target/dbeam-core-shaded.jar \
  com.spotify.dbeam.jobs.JdbcAvroJob \
  ...
```

## Using as a library


To include DBeam library in a mvn project add the following dependency in `pom.xml`:

```xml
<dependency>
  <groupId>com.spotify</groupId>
  <artifactId>dbeam-core</artifactId>
  <version>${dbeam.version}</version>
</dependency>
```


To include DBeam library in a SBT project add the following dependency in `build.sbt`:

```sbt
  libraryDependencies ++= Seq(
   "com.spotify" % "dbeam-core" % dbeamVersion
  )
```

## Development

Make sure you have [mvn](https://maven.apache.org/) installed.
For editor, [IntelliJ IDEA][idea] is recommended.

To test and verify changes during development, run:

```sh
mvn verify
```

Or:


```sh
mvn verify -Pcoverage
```

This project adheres to the [Open Code of Conduct][code-of-conduct]. By participating, you are
expected to honor this code.

## Release

Every push to master will deploy a snapshot version to Sonatype.
You can check the deployment in the following links:

- https://github.com/spotify/dbeam/actions
- https://oss.sonatype.org/#nexus-search;quick~dbeam-core

## Future roadmap

DBeam is mature, maintained and used in production since August 2017. No major features or development planned.
Like Redis/[Redict](https://andrewkelley.me/post/redis-renamed-to-redict.html), DBeam can be considered a finished product.

> It can be maintained for decades to come with minimal effort. It can continue to provide a high amount of value for a low amount of labor.


---

## License

Copyright 2016-2022 Spotify AB.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

---

[code-of-conduct]: https://github.com/spotify/code-of-conduct/blob/master/code-of-conduct.md
[idea]: https://www.jetbrains.com/idea/download/
[beam]: https://beam.apache.org/
