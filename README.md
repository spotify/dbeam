
DBeam
=======

[![Build Status](https://travis-ci.org/spotify/dbeam.svg?branch=master)](https://travis-ci.org/spotify/dbeam)
[![codecov.io](https://codecov.io/github/spotify/dbeam/coverage.svg?branch=master)](https://codecov.io/github/spotify/dbeam?branch=master)
[![GitHub license](https://img.shields.io/github/license/spotify/dbeam.svg)](./LICENSE)
[![Maven Central](https://img.shields.io/maven-central/v/com.spotify/dbeam-core_2.12.svg)](https://maven-badges.herokuapp.com/maven-central/com.spotify/dbeam-core_2.12)

A connector tool to extract data from SQL databases and import into [GCS](https://cloud.google.com/storage/) using [Apache Beam](https://beam.apache.org/).

This tool is runnable locally, or on any other backend supported by Apache Beam, e.g. [Cloud Dataflow](https://cloud.google.com/dataflow/).

**DEVELOPMENT STATUS: Alpha. Usable in production already.**

## Overview

DBeam is a [Scio](scio) based single threaded pipeline that reads
all the data from single SQL database table, and converts the data into [Avro](https://avro.apache.org/) and stores it into
appointed location, usually in GCS. Scio runs on [Apache Beam](https://beam.apache.org/).

DBeam requires the database credentials, the database table name to read, and the output location
to store the extracted data into. DBeam first makes a single select into the target table with
limit one to infer the table schema. After the schema is created the job will be launched which
simply streams the table contents via JDBC into target location as Avro.

## `dbeam` Java/Scala package features

- Support both PostgreSQL and MySQL JDBC connectors
- Supports CloudSQL managed databases
- Currently output only in Avro format
- Read password from a mounted password file (`--passwordFile`)
- Can filter only records of the current day with the `--partitionColumn` parameter
- Check and fail on too old partition dates. Snapshot dumps are not filtered by a given date/partition, when running for a too old partition, the job fails to avoid new data in old partitions. (can be disabled with `--skipPartitionCheck`)
- It has dependency on Apache Beam SDK and scio-core. No internal dependencies to Spotify. Easy to open source.

### `dbeam` arguments

- `--connectionUrl`: the JDBC connection url to perform the dump
- `--table`: the database table to query and perform the dump
- `--output`: the path to store the output
- `--username`: the database user name
- `--password`: the database password
- `--passwordFile`: a path to a local file containing the database password
- `--limit`: limit the output number of rows, indefinite by default
- `--avroSchemaNamespace`: the namespace of the generated avro schema, `"dbeam_generated"` by default
- `--partitionColumn`: the name of a date/timestamp column to filter data based on current partition
- `--partition`: the date of the current partition, parsed using [ISODateTimeFormat.localDateOptionalTimeParser](http://www.joda.org/joda-time/apidocs/org/joda/time/format/ISODateTimeFormat.html#localDateOptionalTimeParser--)
- `--partitionPeriod`: the period in which dbeam runs, used to filter based on current partition and also to check if executions are being run for a too old partition
- `--skipPartitionCheck`: when partition column is not specified, by default fail when the partition parameter is not too old; use this avoid this behavior
- `--minPartitionPeriod`: the minimum partition required for the job not to fail (when partition column is not specified), by default `now() - 2*partitionPeriod`

## Building

Build with SBT package to get a jar that you can run with `java -cp`. Notice that this won't
create a fat jar, which means that you need to include dependencies on the class path.

```sh
sbt package
```

You can also build the project with SBT pack, which will create a `dbeam-pack/target/pack`
directory with all the dependencies, and also a shell script to run DBeam.

```sh
sbt pack
```

Now you can run the script directly from created dbeam-pack directory:

```sh
./dbeam-pack/target/pack/bin/jdbc-avro-job
```

TODO: We will be improving the packaging and releasing process shortly.

## Examples

```
java -cp CLASS_PATH dbeam-core_2.12.jar com.spotify.dbeam.JdbcAvroJob \
  --output=gs://my-testing-bucket-name/ \
  --username=my_database_username \
  --password=secret \
  --connectionUrl=jdbc:postgresql://some.database.uri.example.org:5432/my_database \
  --table=my_table
```

For CloudSQL:
```
java -cp CLASS_PATH dbeam-core_2.12.jar com.spotify.dbeam.JdbcAvroJob \
  --output=gs://my-testing-bucket-name/ \
  --username=my_database_username \
  --password=secret \
  --connectionUrl=jdbc:postgresql://google/database?socketFactory=com.google.cloud.sql.postgres.SocketFactory&socketFactoryArg=project:region:cloudsql-instance \
  --table=my_table
```

- Replace postgres with mysql if you are using MySQL.
- More details can be found at [CloudSQL JDBC SocketFactory](https://github.com/GoogleCloudPlatform/cloud-sql-jdbc-socket-factory)

To validate a data extraction one can run:

```sh
java -cp CLASS_PATH dbeam-core_2.12.jar com.spotify.dbeam.JdbcAvroJob \
  --output=gs://my-testing-bucket-name/ \
  --username=my_database_username \
  --password=secret \
  --connectionUrl=jdbc:postgresql://some.database.uri.example.org:5432/my_database \
  --table=my_table \
  --limit=10 \
  --skipPartitionCheck
```

## Requirements

DBeam is built on top of [Scio][scio] and supports both Scala 2.12 and 2.11.

To include DBeam library in a SBT project add the following in build.sbt:

```sbt
  libraryDependencies ++= Seq(
   "com.spotify" %% "dbeam-core" % dbeamVersion
  )
```

## Development

Make sure you have [sbt][sbt] installed. For editor, [IntelliJ IDEA][idea] with [scala plugin][scala-plugin] is recommended.

To test and verify during development, run:

```
sbt clean scalastyle test:scalastyle coverage test coverageReport coverageAggregate
```

This project adheres to the [Open Code of Conduct][code-of-conduct]. By participating, you are
expected to honor this code.

---

## License

Copyright 2016-2017 Spotify AB.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

---

[code-of-conduct]: https://github.com/spotify/code-of-conduct/blob/master/code-of-conduct.md
[sbt]: http://www.scala-sbt.org/
[idea]: https://www.jetbrains.com/idea/download/
[scala-plugin]: https://plugins.jetbrains.com/plugin/1347-scala
[beam]: https://beam.apache.org/
[scio]: https://github.com/spotify/scio
