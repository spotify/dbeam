#!/usr/bin/env bash

# fail on error
set -o errexit
set -o nounset
set -o pipefail

readonly SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
readonly PROJECT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." >/dev/null && pwd)"

# This file contatins psql views with complex types to validate and troubleshoot dbeam

PSQL_DOCKER_IMAGE=postgres:10
PSQL_USER=postgres
PSQL_PASSWORD=mysecretpassword
PSQL_DB=dbeam_test
DOCKER_NETWORK=dbeam1-network
JAVA_DOCKER_IMAGE=gcr.io/distroless/java:11

startPostgres() {
  set -o xtrace
  docker --version
  docker network create "$DOCKER_NETWORK" || true

  mkdir -p /tmp/pgdata
  docker run --detach --name dbeam-postgres \
    --net "$DOCKER_NETWORK" \
    --env "POSTGRES_DB=dbeam_test" \
    --env "POSTGRES_PASSWORD=mysecretpassword" \
    --mount="type=bind,source=/tmp/pgdata,target=/var/lib/postgresql/data" \
    --publish="54321:5432/tcp" "$PSQL_DOCKER_IMAGE" || docker start dbeam-postgres
  sleep 1
  docker ps
  docker logs dbeam-postgres
  # https://stackoverflow.com/questions/35069027/docker-wait-for-postgresql-to-be-running
  time docker run --interactive --rm \
    --net "$DOCKER_NETWORK" \
    --env "PGPASSWORD=mysecretpassword" \
    "$PSQL_DOCKER_IMAGE" \
    timeout 45s bash -xc 'until psql -h dbeam-postgres -U postgres dbeam_test -c "select 1"; do sleep 1; done; echo "psql up and running.."'
  sleep 3
  time docker run --interactive --rm \
    --net "$DOCKER_NETWORK" \
    --env "PGPASSWORD=mysecretpassword" \
    "$PSQL_DOCKER_IMAGE" \
    timeout 30s psql -h dbeam-postgres -U postgres dbeam_test < "$SCRIPT_PATH/ddl.sql"
  timeout 1 bash -c "cat < /dev/null > /dev/tcp/0.0.0.0/54321" && echo "success"
}

dockerClean() {
  docker rm -f dbeam-postgres
  docker network rm "$DOCKER_NETWORK"
}

JAVA_OPTS=(
-XX:+UseParallelGC
-Xmx1g
-Xms1g
)

pack() {
  java -version
  # create a fat jar
  (cd "$PROJECT_PATH"; mvn package -Ppack -DskipTests -Dmaven.test.skip=true -Dmaven.site.skip=true -Dmaven.javadoc.skip=true)
}

run_docker_dbeam() {
  time docker run --interactive --rm \
    --net="$DOCKER_NETWORK" \
    --mount="type=bind,source=$PROJECT_PATH/dbeam-core/target,target=/dbeam" \
    --memory=1G \
    --entrypoint=/usr/bin/java \
    "$JAVA_DOCKER_IMAGE" \
    "${JAVA_OPTS[@]}" -cp /dbeam/dbeam-core-shaded.jar com.spotify.dbeam.jobs.BenchJdbcAvroJob "$@"
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
}

runSuite() {
  table=demo_table
  BINARY_TRANSFER='false' runDBeamDockerCon --executions=3 --avroCodec=deflate1
  BINARY_TRANSFER='false' runDBeamDockerCon --executions=3 --avroCodec=zstandard1
  BINARY_TRANSFER='false' runDBeamDockerCon --executions=3 --avroCodec=deflate1 --queryParallelism=5 --splitColumn=row_number
}

light() {
  pack
  table=demo_table
  BINARY_TRANSFER='false' runDBeamDockerCon --executions=3 --avroCodec=deflate1
}


main() {
  if [[ $# -gt 0 ]]; then
    "$@"
  else
    # pack  # assume pack already ran before
    time startPostgres

    runSuite
    dockerClean
  fi
}

main "$@"
