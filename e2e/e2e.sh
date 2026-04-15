#!/usr/bin/env bash

shopt -s expand_aliases
source ~/.bashrc

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
    --mount="type=bind,source=/tmp/pgdata,target=/var/lib/postgresql/data" \
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

runDBeamParquetDockerCon() {
  OUTPUT="$SCRIPT_PATH/results/testn/parquet-$(date +%FT%H%M%S)/"
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
  OUTPUT_FILE=$(ls ${OUTPUT}run_0/*.parquet | head -n 1)
  echo "Parquet output: $OUTPUT_FILE ($(wc -c < "$OUTPUT_FILE" | tr -d ' ') bytes)"
  parquet-tools head -n 5 "$OUTPUT_FILE" || echo "parquet-tools not available, skipping content validation"
  parquet-tools schema "$OUTPUT_FILE" || echo "parquet-tools not available, skipping schema validation"

  # Verify parquet.avro.schema is present in footer metadata
  AVRO_SCHEMA_META=$(parquet-tools meta "$OUTPUT_FILE" 2>/dev/null | grep "parquet.avro.schema" || true)
  if [[ -n "$AVRO_SCHEMA_META" ]]; then
    echo "OK: parquet.avro.schema found in footer metadata"
    # Sanity check: should contain "type" and "record" (valid Avro JSON)
    echo "$AVRO_SCHEMA_META" | grep -q '"type"' && echo "OK: Avro schema contains type field" || echo "WARN: Avro schema may be malformed"
  else
    echo "FAIL: parquet.avro.schema NOT found in footer metadata"
    exit 1
  fi
}

runSuite() {
  table=demo_table
  BINARY_TRANSFER='false' runDBeamDockerCon --executions=3 --avroCodec=deflate1
  BINARY_TRANSFER='false' runDBeamDockerCon --executions=3 --avroCodec=zstandard1
  BINARY_TRANSFER='false' runDBeamDockerCon --executions=3 --avroCodec=deflate1 --queryParallelism=5 --splitColumn=row_number
  BINARY_TRANSFER='false' runDBeamDockerCon --executions=3 --avroCodec=deflate1 --arrayMode=bytes
  BINARY_TRANSFER='false' runDBeamDockerCon --executions=3 --avroCodec=deflate1 --arrayMode=typed_postgres
}

runParquetSuite() {
  table=demo_table
  BINARY_TRANSFER='false' runDBeamParquetDockerCon --executions=3
  BINARY_TRANSFER='false' runDBeamParquetDockerCon --executions=3 --queryParallelism=5 --splitColumn=row_number
}

light() {
  pack
  table=demo_table
  BINARY_TRANSFER='false' runDBeamDockerCon --executions=3 --avroCodec=deflate1 --arrayMode=typed_postgres
}

lightParquet() {
  pack
  table=demo_table
  BINARY_TRANSFER='false' runDBeamParquetDockerCon --executions=3 --avroCodec=snappy
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
