#!/usr/bin/env bash

shopt -s expand_aliases
if [[ -f ~/.bashrc ]]; then
  source ~/.bashrc
fi

# fail on error
set -o errexit
set -o nounset
set -o pipefail

readonly SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
readonly PROJECT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." >/dev/null && pwd)"

# This file contatins psql views with complex types to validate and troubleshoot dbeam

PSQL_DOCKER_IMAGE=postgres:18
PSQL_USER=postgres
PSQL_PASSWORD=tempandnotasecret
PSQL_DB=dbeam_test
DOCKER_NETWORK=dbeam1-network
JAVA_DOCKER_IMAGE=gcr.io/distroless/java17-debian12

startPostgres() {
  set -o xtrace
  docker --version
  docker network create "$DOCKER_NETWORK" || true

  rm -rf /tmp/pgdata || true

  mkdir -p /tmp/pgdata
  docker run --detach --name dbeam-postgres \
    --net "$DOCKER_NETWORK" \
    --env "POSTGRES_DB=dbeam_test" \
    --env "POSTGRES_PASSWORD=$PSQL_PASSWORD" \
    --mount="type=bind,source=/tmp/pgdata,target=/var/lib/postgresql" \
    --publish="54321:5432/tcp" "$PSQL_DOCKER_IMAGE" || docker start dbeam-postgres
  sleep 1
  docker ps
  docker logs dbeam-postgres
  # https://stackoverflow.com/questions/35069027/docker-wait-for-postgresql-to-be-running
  time docker run --interactive --rm \
    --net "$DOCKER_NETWORK" \
    --env "PGPASSWORD=$PSQL_PASSWORD" \
    "$PSQL_DOCKER_IMAGE" \
    timeout 45s bash -xc 'until psql -h dbeam-postgres -U postgres dbeam_test -c "select 1"; do sleep 1; done; echo "psql up and running.."'
  sleep 3
  time docker run --interactive --rm \
    --net "$DOCKER_NETWORK" \
    --env "PGPASSWORD=$PSQL_PASSWORD" \
    "$PSQL_DOCKER_IMAGE" \
    timeout 30s psql -h dbeam-postgres -U postgres dbeam_test < "$SCRIPT_PATH/ddl.sql"
  timeout 1 bash -c "cat < /dev/null > /dev/tcp/0.0.0.0/54321" && echo "success"
}

dockerClean() {
  docker rm -f dbeam-postgres || true
  docker network rm "$DOCKER_NETWORK" || true
}

JAVA_OPTS=(
-XX:+UseParallelGC
-Xmx1g
-Xms1g
)

pack() {
  java -version
  # create fat jars
  (cd "$PROJECT_PATH"; mvn package -Ppack -DskipTests -Dmaven.test.skip=true -Dmaven.site.skip=true -Dmaven.javadoc.skip=true)
}

run_docker_dbeam() {
  time docker run --interactive --rm \
    --net="$DOCKER_NETWORK" \
    --mount="type=bind,source=$PROJECT_PATH/dbeam-core/target,target=/dbeam" \
    --mount="type=bind,source=$SCRIPT_PATH,target=$SCRIPT_PATH" \
    --memory=1G \
    --entrypoint=/usr/bin/java \
    "$JAVA_DOCKER_IMAGE" \
    "${JAVA_OPTS[@]}" -cp /dbeam/dbeam-core-shaded.jar com.spotify.dbeam.jobs.BenchJdbcAvroJob "$@"
}

run_docker_dbeam_parquet() {
  time docker run --interactive --rm \
    --net="$DOCKER_NETWORK" \
    --mount="type=bind,source=$PROJECT_PATH/dbeam-parquet/target,target=/dbeam" \
    --mount="type=bind,source=$SCRIPT_PATH,target=$SCRIPT_PATH" \
    --memory=1G \
    --entrypoint=/usr/bin/java \
    "$JAVA_DOCKER_IMAGE" \
    "${JAVA_OPTS[@]}" -cp /dbeam/dbeam-parquet-shaded.jar com.spotify.dbeam.parquet.BenchJdbcParquetJob "$@"
}

runDBeamDockerCon() {
  OUTPUT="$SCRIPT_PATH/results/testn/$(date +%FT%H%M%S)/"
  set -o xtrace
  time \
    run_docker_dbeam \
    --skipPartitionCheck \
    --targetParallelism=1 \
    "--connectionUrl=jdbc:postgresql://dbeam-postgres:5432/$PSQL_DB?binaryTransfer=${BINARY_TRANSFER:-false}" \
    "--username=$PSQL_USER" \
    "--password=$PSQL_PASSWORD" \
    "--table=${table:-demo_table}" \
    "--partition=$(date +%F)" \
    "--output=$OUTPUT" \
    "--minRows=${minRows:-1000000}" \
    "$@" 2>&1 | tee -a /tmp/debeam_e2e.log
  OUTPUT_FILE=$(ls ${OUTPUT}run_0/*.avro | head -n 1)
  avro-tools tojson --head=5 $OUTPUT_FILE
}

run_docker_dbeam_jdbc() {
  time docker run --interactive --rm \
    --net="$DOCKER_NETWORK" \
    --mount="type=bind,source=$PROJECT_PATH/dbeam-core/target,target=/dbeam" \
    --mount="type=bind,source=$SCRIPT_PATH,target=$SCRIPT_PATH" \
    --memory=1G \
    --entrypoint=/usr/bin/java \
    "$JAVA_DOCKER_IMAGE" \
    "${JAVA_OPTS[@]}" \
    -cp /dbeam/dbeam-core-shaded.jar \
    com.spotify.dbeam.jobs.JdbcAvroJob "$@"
}

run_docker_dbeam_parquet_jdbc() {
  time docker run --interactive --rm \
    --net="$DOCKER_NETWORK" \
    --mount="type=bind,source=$PROJECT_PATH/dbeam-parquet/target,target=/dbeam" \
    --mount="type=bind,source=$SCRIPT_PATH,target=$SCRIPT_PATH" \
    --memory=1G \
    --entrypoint=/usr/bin/java \
    "$JAVA_DOCKER_IMAGE" \
    "${JAVA_OPTS[@]}" \
    -cp /dbeam/dbeam-parquet-shaded.jar \
    com.spotify.dbeam.parquet.JdbcParquetJob "$@"
}

# Classpath for the Java Parquet verifiers. The shaded jar includes parquet-avro, but
# AvroParquetReader also needs hadoop-mapreduce-client-core, which is test-scoped, so add the
# test classpath. Computed once per e2e run.
parquetVerifierClasspath() {
  local VERIFY_CP_FILE
  VERIFY_CP_FILE="$SCRIPT_PATH/results/verify_parquet_classpath.txt"
  if [[ ! -s "$VERIFY_CP_FILE" ]]; then
    mkdir -p "$SCRIPT_PATH/results"
    (cd "$PROJECT_PATH"; mvn --quiet -pl dbeam-parquet dependency:build-classpath \
      -Dmdep.includeScope=test "-Dmdep.outputFile=$VERIFY_CP_FILE") >&2
  fi
  echo "$PROJECT_PATH/dbeam-parquet/target/dbeam-parquet-shaded.jar:$(cat "$VERIFY_CP_FILE")"
}

# Reads every Parquet file of an export (all run_N directories) through its footer Avro schema
# and checks row counts, row_number coverage and demo_table values.
# Usage: verifyParquetOutput <outputDir> <rowsPerRun> [VerifyParquetOutput flags...]
verifyParquetOutput() {
  java \
    --class-path "$(parquetVerifierClasspath)" \
    "$SCRIPT_PATH/VerifyParquetOutput.java" \
    "$@"
}

runTimestampMicrosTest() {
  echo "=== timestamp-micros e2e test ==="
  local OUTPUT
  OUTPUT="$SCRIPT_PATH/results/timestamp_micros/"
  rm -rf "$OUTPUT"
  set -o xtrace
  run_docker_dbeam_jdbc \
    --skipPartitionCheck \
    --runner=DirectRunner \
    "--connectionUrl=jdbc:postgresql://dbeam-postgres:5432/$PSQL_DB" \
    "--username=$PSQL_USER" \
    "--password=$PSQL_PASSWORD" \
    "--table=timestamp_micros_test" \
    "--output=$OUTPUT" \
    "--avroSchemaNamespace=dbeam_generated" \
    --useLogicalTypes \
    --useTimestampMicros \
    --avroCodec=deflate1
  set +o xtrace

  local AVRO_FILE
  AVRO_FILE=$(find "$OUTPUT" -name '*.avro' | head -n 1)
  if [[ -z "$AVRO_FILE" ]]; then
    echo "FAIL: no avro output file found"
    return 1
  fi

  echo "--- verifying microsecond precision ---"
  java \
    --class-path "$PROJECT_PATH/dbeam-core/target/dbeam-core-shaded.jar" \
    "$SCRIPT_PATH/VerifyTimestampMicros.java" \
    "$AVRO_FILE"
  echo "PASS: timestamp values retain microsecond precision"
}

runParquetTimestampMicrosTest() {
  echo "=== timestamp-micros e2e test (parquet) ==="
  local OUTPUT
  OUTPUT="$SCRIPT_PATH/results/timestamp_micros_parquet/"
  rm -rf "$OUTPUT"
  set -o xtrace
  run_docker_dbeam_parquet_jdbc \
    --skipPartitionCheck \
    --runner=DirectRunner \
    "--connectionUrl=jdbc:postgresql://dbeam-postgres:5432/$PSQL_DB" \
    "--username=$PSQL_USER" \
    "--password=$PSQL_PASSWORD" \
    "--table=timestamp_micros_test" \
    "--output=$OUTPUT" \
    "--avroSchemaNamespace=dbeam_generated" \
    --useLogicalTypes \
    --useTimestampMicros
  set +o xtrace

  echo "--- verifying microsecond precision (parquet, all output files) ---"
  java \
    --class-path "$(parquetVerifierClasspath)" \
    "$SCRIPT_PATH/VerifyParquetTimestampMicros.java" \
    "$OUTPUT" "${OUTPUT}_AVRO_SCHEMA.avsc"
  echo "PASS: parquet timestamp values retain microsecond precision"
  echo "PASS: footer parquet.avro.schema equals _AVRO_SCHEMA.avsc"
}

timestampMicros() {
  dockerClean
  trap dockerClean EXIT
  pack
  startPostgres
  runTimestampMicrosTest
  runParquetTimestampMicrosTest
}

# Usage: [VERIFY_ARGS="--logicalTypes ..."] runDBeamParquetDockerCon <dbeam args...>
# VERIFY_ARGS are passed to VerifyParquetOutput and must describe the dbeam args.
runDBeamParquetDockerCon() {
  OUTPUT="$SCRIPT_PATH/results/testn/parquet-$(date +%FT%H%M%S)/"
  local verify_args=()
  read -r -a verify_args <<< "${VERIFY_ARGS:-}"
  set -o xtrace
  time \
    run_docker_dbeam_parquet \
    --skipPartitionCheck \
    --targetParallelism=1 \
    "--connectionUrl=jdbc:postgresql://dbeam-postgres:5432/$PSQL_DB?binaryTransfer=${BINARY_TRANSFER:-false}" \
    "--username=$PSQL_USER" \
    "--password=$PSQL_PASSWORD" \
    "--table=${table:-demo_table}" \
    "--partition=$(date +%F)" \
    "--output=$OUTPUT" \
    "--minRows=${minRows:-1000000}" \
    "$@" 2>&1 | tee -a /tmp/debeam_e2e.log
  set +o xtrace
  verifyParquetOutput "$OUTPUT" "${minRows:-1000000}" ${verify_args[@]+"${verify_args[@]}"}
}

runSuite() {
  table=demo_table
  BINARY_TRANSFER='false' runDBeamDockerCon --executions=3 --avroCodec=deflate1
  BINARY_TRANSFER='false' runDBeamDockerCon --executions=3 --avroCodec=zstandard1
  BINARY_TRANSFER='false' runDBeamDockerCon --executions=3 --avroCodec=deflate1 --queryParallelism=5 --splitColumn=row_number
  BINARY_TRANSFER='false' runDBeamDockerCon --executions=3 --avroCodec=deflate1 --arrayMode=bytes
  BINARY_TRANSFER='false' runDBeamDockerCon --executions=3 --avroCodec=deflate1 --arrayMode=typed_postgres
  runTimestampMicrosTest
  runParquetTimestampMicrosTest
}

# Parquet counterparts of runSuite, plus the logical type, uuid and --dataOnly variants.
runParquetSuite() {
  table=demo_table
  BINARY_TRANSFER='false' runDBeamParquetDockerCon --executions=3
  BINARY_TRANSFER='false' runDBeamParquetDockerCon --executions=1 --parquetCodec=gzip
  BINARY_TRANSFER='false' runDBeamParquetDockerCon --executions=1 --parquetCodec=zstd
  BINARY_TRANSFER='false' runDBeamParquetDockerCon --executions=3 --queryParallelism=5 --splitColumn=row_number
  VERIFY_ARGS="--arrayMode=bytes" \
    BINARY_TRANSFER='false' runDBeamParquetDockerCon --executions=1 --arrayMode=bytes
  BINARY_TRANSFER='false' runDBeamParquetDockerCon --executions=1 --arrayMode=typed_postgres
  VERIFY_ARGS="--logicalTypes" \
    BINARY_TRANSFER='false' runDBeamParquetDockerCon --executions=1 --useLogicalTypes
  VERIFY_ARGS="--logicalTypes --micros" \
    BINARY_TRANSFER='false' runDBeamParquetDockerCon --executions=1 --useLogicalTypes \
    --useTimestampMicros --queryParallelism=5 --splitColumn=row_number
  VERIFY_ARGS="--logicalTypes" \
    BINARY_TRANSFER='false' runDBeamParquetDockerCon --executions=1 --useLogicalTypes \
    --parquetNativeTypes
  VERIFY_ARGS="--dataOnly" \
    BINARY_TRANSFER='false' runDBeamParquetDockerCon --executions=1 --dataOnly
}

light() {
  pack
  table=demo_table
  BINARY_TRANSFER='false' runDBeamDockerCon --executions=3 --avroCodec=deflate1 --arrayMode=typed_postgres
}

lightParquet() {
  pack
  table=demo_table
  BINARY_TRANSFER='false' runDBeamParquetDockerCon --executions=3 --parquetCodec=snappy
}


main() {
  if [[ $# -gt 0 ]]; then
    "$@"
  else
    dockerClean
    # pack  # assume pack already ran before
    time startPostgres

    runSuite
    runParquetSuite
    dockerClean
  fi
}

main "$@"
