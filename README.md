
DBeam
=======

[![Build Status](https://travis-ci.org/spotify/dbeam.svg?branch=master)](https://travis-ci.org/spotify/dbeam)
[![codecov.io](https://codecov.io/github/spotify/dbeam/coverage.svg?branch=master)](https://codecov.io/github/spotify/dbeam?branch=master)
[![GitHub license](https://img.shields.io/github/license/spotify/dbeam.svg)](./LICENSE)
[![Maven Central](https://img.shields.io/maven-central/v/com.spotify/dbeam-core.svg)](https://maven-badges.herokuapp.com/maven-central/com.spotify/dbeam-core)

A connector tool to extract data from SQL databases and import into [GCS](https://cloud.google.com/storage/) using [Apache Beam](https://beam.apache.org/).

This tool is runnable locally, or on any other backend supported by Apache Beam, e.g. [Cloud Dataflow](https://cloud.google.com/dataflow/).

**DEVELOPMENT STATUS: Alpha. Usable in production already.**

## Overview

DBeam is tool based that reads all the data from single SQL database table,
converts the data into [Avro](https://avro.apache.org/) and stores it into
appointed location, usually in GCS.
It runs as a single threaded [Apache Beam](https://beam.apache.org/) pipeline.

DBeam requires the database credentials, the database table name to read, and the output location
to store the extracted data into. DBeam first makes a single select into the target table with
limit one to infer the table schema. After the schema is created the job will be launched which
simply streams the table contents via JDBC into target location as Avro.

## dbeam-core package features

- Support both PostgreSQL and MySQL JDBC connectors
- Supports [Google CloudSQL](https://cloud.google.com/sql/) managed databases
- Currently output only to Avro format
- Reads database from an external password file (`--passwordFile`) or an external [KMS](https://cloud.google.com/kms/) encrypted password file (`--passwordFileKmsEncrypted`)
- Can filter only records of the current day with the `--partitionColumn` parameter
- Check and fail on too old partition dates. Snapshot dumps are not filtered by a given date/partition, when running for a too old partition, the job fails to avoid new data in old partitions. (can be disabled with `--skipPartitionCheck`)
- Implemented as [Apache Beam SDK](https://beam.apache.org/) pipeline, supporting any of its [runners](https://beam.apache.org/documentation/runners/capability-matrix/) (tested with `DirectRunner` and `DataflowRunner`)

### dbeam command line arguments

- `--connectionUrl`: the JDBC connection url to perform the dump
- `--table`: the database table to query and perform the dump
- `--output`: the path to store the output
- `--username`: the database user name
- `--password`: the database password
- `--passwordFile`: a path to a local file containing the database password
- `--limit`: limit the output number of rows, indefinite by default
- `--avroSchemaNamespace`: the namespace of the generated avro schema, `"dbeam_generated"` by default
- `--exportTimeout`: maximum time the export can take, after this timeout the job is cancelled. Default is `PT0S` (no timeout).
- `--partitionColumn`: the name of a date/timestamp column to filter data based on current partition
- `--partition`: the date of the current partition, parsed using [ISODateTimeFormat.localDateOptionalTimeParser](http://www.joda.org/joda-time/apidocs/org/joda/time/format/ISODateTimeFormat.html#localDateOptionalTimeParser--)
- `--partitionPeriod`: the period in which dbeam runs, used to filter based on current partition and also to check if executions are being run for a too old partition
- `--skipPartitionCheck`: when partition column is not specified, by default fail when the partition parameter is not too old; use this avoid this behavior
- `--minPartitionPeriod`: the minimum partition required for the job not to fail (when partition column is not specified), by default `now() - 2*partitionPeriod`
- `--queryParallelism`: number of queries to generate to extract in parallel. Generates one query if nothing is specified. Must be used split column defined.
- `--splitColumn`: a long / integer column which is used to determine bounds for generating parallel queries. Must be used with parallelism defined.

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
mvn validate
```

In order to create a jar with all dependencies under `./dbeam-core/target/dbeam-core-shaded.jar` run the following:

```sh
mvn clean package -Ppack
```

## Usage examples

Using java from the command line:

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

- Replace postgres with mysql if you are using MySQL.
- More details can be found at [CloudSQL JDBC SocketFactory](https://github.com/GoogleCloudPlatform/cloud-sql-jdbc-socket-factory)

For MySQL, set `--fetchSize=-2147483648` (`java.lang.Integer.MIN_INT`) to signal to the driver to stream result sets row-by-row, avoiding Out of Memory issues. Read more about MySQL JDBC driver [here](https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-implementation-notes.html).

To run a cheap data extraction, as a way to validate, one can run:

```sh
java -cp ./dbeam-core/target/dbeam-core-shaded.jar \
  com.spotify.dbeam.jobs.JdbcAvroJob \
  --output=gs://my-testing-bucket-name/ \
  --username=my_database_username \
  --password=secret \
  --connectionUrl=jdbc:postgresql://some.database.uri.example.org:5432/my_database \
  --table=my_table \
  --limit=10 \
  --skipPartitionCheck
```

### Password configuration

Database password can be configured by simply passing `--password=writepasswordhere`, `--passwordFile=/path/to/file/containing/password` or `--passwordFile=gs://gcs-bucket/path/to/file/containing/password`.

A more robust configuration is to point to a [Google KMS](https://cloud.google.com/kms/) encrypted file.
DBeam will try to decrypt using KMS if the file ends with `.encrypted` (e.g. `--passwordFileKmsEncrypted=gs://gcs-bucket/path/to/db-password.encrypted`).

The file should contain a base64 encoded encrypted content.
It can be generated using [`gcloud`](https://cloud.google.com/sdk/gcloud/) like the following:

```sh
echo "super_secret_password" \
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
mvn validate
```

Or:


```sh
mvn validate -Pcoverage
```

This project adheres to the [Open Code of Conduct][code-of-conduct]. By participating, you are
expected to honor this code.

## Release

Every push to master will deploy a snapshot version to Sonatype.
You can check the deployment in the following links:

- https://travis-ci.org/spotify/dbeam/builds
- https://oss.sonatype.org/#nexus-search;quick~dbeam-core

---

## License

Copyright 2016-2019 Spotify AB.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

---

[code-of-conduct]: https://github.com/spotify/code-of-conduct/blob/master/code-of-conduct.md
[idea]: https://www.jetbrains.com/idea/download/
[beam]: https://beam.apache.org/
