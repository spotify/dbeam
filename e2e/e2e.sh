#!/usr/bin/env bash

# fail on error
set -o errexit
set -o nounset
set -o pipefail

# This file contatins psql views with complex types to validate and troubleshoot dbeam

PSQL_USER=postgres
PSQL_PASSWORD=mysecretpassword
PSQL_DB=dbeam_test

startPostgres() {
  docker run --name dbeam-postgres \
    -e POSTGRES_DB=dbeam_test \
    -e POSTGRES_PASSWORD=mysecretpassword \
    -v /tmp/pgdata:/var/lib/postgresql/data \
    -p 54321:5432/tcp -d postgres:10 || true
  # https://stackoverflow.com/questions/35069027/docker-wait-for-postgresql-to-be-running
  docker run -it --rm --link dbeam-postgres:postgres -e PGPASSWORD=mysecretpassword postgres:10.7 timeout 45s bash -ic 'until psql -h postgres -U postgres dbeam_test -c "select 1"; do sleep 1; done; echo "psql up and running.."'
  sleep 3
  cat ./ddl.sql \
    | docker run -i --rm --link dbeam-postgres:postgres -e PGPASSWORD=mysecretpassword postgres:10 psql -h postgres -U postgres dbeam_test
  echo '\d' | docker run -i --rm --link dbeam-postgres:postgres -e PGPASSWORD=mysecretpassword postgres:10 psql -h postgres -U postgres dbeam_test
}

dockerClean() {
  docker rm -f dbeam-postgres
}

#"-XX:+PrintGCApplicationStoppedTime"
#-agentpath:/Applications/VisualVM.app/Contents/profiler/lib/deployed/jdk16/mac/libprofilerinterface.jnilib=/Applications/VisualVM.app/Contents/profiler/lib,5141
export JAVA_OPTS="
-XX:+DisableExplicitGC
-XX:+PrintGCDetails
-XX:+PrintGCApplicationStoppedTime
-XX:+PrintGCApplicationConcurrentTime
-XX:+PrintGCDateStamps
-Xloggc:gclog.log
-XX:+UseGCLogFileRotation
-XX:NumberOfGCLogFiles=5
-XX:GCLogFileSize=2000k
-XX:+UseParallelGC
-Xmx1g
-Xms1g
"

pack() {
  # create a fat jar
  (cd ..; mvn clean package -Ppack -DskipTests -Dmaven.test.skip=true -Dmaven.site.skip=true -Dmaven.javadoc.skip=true)
}

runFromJar() {
  (set -ex; java $JAVA_OPTS -cp ../dbeam-core/target/dbeam-core-shaded.jar com.spotify.dbeam.jobs.JdbcAvroJob "$@")
}

runDbeamDefault() {
  time \
    runFromJar \
    --skipPartitionCheck \
    --targetParallelism=1 \
    "$@" 2>&1 | tee -a /tmp/out1
}

DOCKER_PSQL_ARGS=(
  "--username=$PSQL_USER"
  "--password=$PSQL_PASSWORD"
  "--connectionUrl=jdbc:postgresql://0.0.0.0:54321/$PSQL_DB?binaryTransfer=${BINARY_TRANSFER:-false}"
  "--table=${table:-demo_table}"
)

runDBeamDockerCon() {
  OUTPUT="./results/testn/$(date +%FT%H%M%S)/"
  runDbeamDefault \
    "${DOCKER_PSQL_ARGS[@]}" \
    "--partition=$(date +%F)" \
    "--output=$OUTPUT" \
    "$@"
}

runScenario() {
  for ((i=1;i<=3;i++)); do
    runDBeamDockerCon "${@:2}"
    jq -r "[\"$1\", .recordCount, .writeElapsedMs, .msPerMillionRows, .bytesWritten?, (.bytesWritten? / .writeElapsedMs | . * 1000 | floor | . / 1000)] | @tsv" < "$OUTPUT/_METRICS.json" >> ./bench_dbeam_results
  done
}

runSuite() {
  printf 'scenario\t\trecords\twriteElapsedMs\tmsPerMillionRows\tbytesWritten\tkBps\n' >> ./bench_dbeam_results
  table=demo_table
  BINARY_TRANSFER='false' runScenario "deflate1t5" --avroCodec=deflate1
  BINARY_TRANSFER='false' runScenario "||query" --avroCodec=deflate1 --queryParallelism=5 --splitColumn=row_number
}

light() {
  pack
  printf 'scenario\t\trecords\twriteElapsedMs\tmsPerMillionRows\tbytesWritten\n' >> ./bench_dbeam_results
  table=demo_table
  BINARY_TRANSFER='false' runScenario "deflate1t5" --avroCodec=deflate1
  printResults
}


printResults() {
  column -t -s $'\t' < ./bench_dbeam_results | tail -n 20
}

main() {
  if [[ $# -gt 0 ]]; then
    "$@"
  else
    pack
    time startPostgres

    runSuite
    printResults
    dockerClean
  fi
}

main "$@"
