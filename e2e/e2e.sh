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

startPostgres() {
  docker run --name dbeam-postgres \
    -e POSTGRES_DB=dbeam_test \
    -e POSTGRES_PASSWORD=mysecretpassword \
    -v /tmp/pgdata:/var/lib/postgresql/data \
    -p 54321:5432/tcp -d "$PSQL_DOCKER_IMAGE" || docker start dbeam-postgres
  # https://stackoverflow.com/questions/35069027/docker-wait-for-postgresql-to-be-running
  time docker run -i --rm --link dbeam-postgres:postgres -e PGPASSWORD=mysecretpassword "$PSQL_DOCKER_IMAGE" timeout 45s bash -xc 'until psql -h postgres -U postgres dbeam_test -c "select 1"; do sleep 1; done; echo "psql up and running.."'
  sleep 3
  docker run -i --rm --link dbeam-postgres:postgres -e PGPASSWORD=mysecretpassword "$PSQL_DOCKER_IMAGE" psql -h postgres -U postgres dbeam_test < "$SCRIPT_PATH/ddl.sql"
  echo '\d' | docker run -i --rm --link dbeam-postgres:postgres -e PGPASSWORD=mysecretpassword "$PSQL_DOCKER_IMAGE" psql -h postgres -U postgres dbeam_test
  # ensure one can connect to psql server
  timeout 1 bash -c 'cat < /dev/null > /dev/tcp/0.0.0.0/54331'
}

dockerClean() {
  docker rm -f dbeam-postgres
}

#"-XX:+PrintGCApplicationStoppedTime"
#-agentpath:/Applications/VisualVM.app/Contents/profiler/lib/deployed/jdk16/mac/libprofilerinterface.jnilib=/Applications/VisualVM.app/Contents/profiler/lib,5141
export JAVA_OPTS="
-XX:+UseParallelGC
-Xmx1g
-Xms1g
"

pack() {
  # create a fat jar
  (cd "$PROJECT_PATH"; mvn package -Ppack -DskipTests -Dmaven.test.skip=true -Dmaven.site.skip=true -Dmaven.javadoc.skip=true)
}

runFromJar() {
  (set -ex; java $JAVA_OPTS -cp "$PROJECT_PATH"/dbeam-core/target/dbeam-core-shaded.jar com.spotify.dbeam.jobs.BenchJdbcAvroJob "$@")
}

DOCKER_PSQL_ARGS=(
  "--username=$PSQL_USER"
  "--password=$PSQL_PASSWORD"
  "--connectionUrl=jdbc:postgresql://0.0.0.0:54321/$PSQL_DB?binaryTransfer=${BINARY_TRANSFER:-false}"
  "--table=${table:-demo_table}"
)

runDBeamDockerCon() {
  OUTPUT="$SCRIPT_PATH/results/testn/$(date +%FT%H%M%S)/"
  time \
    runFromJar \
    --skipPartitionCheck \
    --targetParallelism=1 \
    "${DOCKER_PSQL_ARGS[@]}" \
    "--partition=$(date +%F)" \
    "--output=$OUTPUT" \
    "$@" 2>&1 | tee -a /tmp/out1
}

runSuite() {
  java -version
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
