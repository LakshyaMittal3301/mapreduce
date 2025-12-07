#!/usr/bin/env bash

#
# map-reduce tests (adapted to bin/ + bin/plugins/ + data/pg/)
#

# un-comment this to run the tests with the Go race detector.
# RACE=-race

if [[ "$OSTYPE" = "darwin"* ]]
then
  if go version | grep 'go1.17.[012345]'
  then
    # -race with plug-ins on x86 MacOS 12 with
    # go1.17 before 1.17.6 sometimes crash.
    RACE=
    echo '*** Turning off -race since it may not work on a Mac'
    echo '    with ' `go version`
  fi
fi

ISQUIET=$1
maybe_quiet() {
    if [ "$ISQUIET" == "quiet" ]; then
      "$@" > /dev/null 2>&1
    else
      "$@"
    fi
}

TIMEOUT=timeout
TIMEOUT2=""
if timeout 2s sleep 1 > /dev/null 2>&1
then
  :
else
  if gtimeout 2s sleep 1 > /dev/null 2>&1
  then
    TIMEOUT=gtimeout
  else
    # no timeout command
    TIMEOUT=
    echo '*** Cannot find timeout command; proceeding without timeouts.'
  fi
fi
if [ "$TIMEOUT" != "" ]
then
  TIMEOUT2=$TIMEOUT
  TIMEOUT2+=" -k 2s 120s "
  TIMEOUT+=" -k 2s 45s "
fi

NREDUCE=10
JOB_ID="test"
COORD_ADDR="localhost:8123"
COORD_LISTEN=":8123"
COORD_PORT="8123"

COORDINATOR_ARGS=(-n-reduce="${NREDUCE}" -job-id="${JOB_ID}" -listen="${COORD_LISTEN}")
WORKER_ARGS=(-coord-addr="${COORD_ADDR}")

kill_port_listener() {
  local pids
  pids=$(lsof -ti tcp:"${COORD_PORT}" 2>/dev/null || true)
  if [ -n "$pids" ]; then
    kill $pids 2>/dev/null || true
  fi
}

cleanup() {
  local jobs_pids
  jobs_pids=$(jobs -p 2>/dev/null)
  if [ -n "$jobs_pids" ]; then
    kill $jobs_pids 2>/dev/null || true
  fi
  kill_port_listener
}

trap cleanup EXIT INT TERM
kill_port_listener

latest_output_dir() {
  ls -td job/*/output 2>/dev/null | head -n1
}

# root/bin/plugins setup
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${ROOT_DIR}/bin"
PLUGIN_DIR="${BIN_DIR}/plugins"

mkdir -p "${BIN_DIR}" "${PLUGIN_DIR}"

# test working directory under tmp/
TMP_ROOT="${ROOT_DIR}/tmp"
WORKDIR="${TMP_ROOT}/mr-tmp"

rm -rf "$WORKDIR"
mkdir -p "$WORKDIR" || exit 1
cd "$WORKDIR" || exit 1
rm -f mr-*

# make sure software is freshly built.
( cd "${ROOT_DIR}/apps" && go clean )
( cd "${ROOT_DIR}" && go clean )

( cd "${ROOT_DIR}/apps" && go build $RACE -buildmode=plugin -o "${PLUGIN_DIR}/wc.so"          wc.go ) || exit 1
( cd "${ROOT_DIR}/apps" && go build $RACE -buildmode=plugin -o "${PLUGIN_DIR}/indexer.so"     indexer.go ) || exit 1
( cd "${ROOT_DIR}/apps" && go build $RACE -buildmode=plugin -o "${PLUGIN_DIR}/mtiming.so"     mtiming.go ) || exit 1
( cd "${ROOT_DIR}/apps" && go build $RACE -buildmode=plugin -o "${PLUGIN_DIR}/rtiming.so"     rtiming.go ) || exit 1
( cd "${ROOT_DIR}/apps" && go build $RACE -buildmode=plugin -o "${PLUGIN_DIR}/jobcount.so"    jobcount.go ) || exit 1
( cd "${ROOT_DIR}/apps" && go build $RACE -buildmode=plugin -o "${PLUGIN_DIR}/early_exit.so"  early_exit.go ) || exit 1
( cd "${ROOT_DIR}/apps" && go build $RACE -buildmode=plugin -o "${PLUGIN_DIR}/crash.so"       crash.go ) || exit 1
( cd "${ROOT_DIR}/apps" && go build $RACE -buildmode=plugin -o "${PLUGIN_DIR}/nocrash.so"     nocrash.go ) || exit 1

go build $RACE -o "${BIN_DIR}/mrcoordinator" "${ROOT_DIR}/cmd/mrcoordinator.go" || exit 1
go build $RACE -o "${BIN_DIR}/mrworker"      "${ROOT_DIR}/cmd/mrworker.go"      || exit 1
go build $RACE -o "${BIN_DIR}/mrsequential"  "${ROOT_DIR}/cmd/mrsequential.go"  || exit 1

failed_any=0

#########################################################
# first word-count

# generate the correct output
"${BIN_DIR}/mrsequential" "${PLUGIN_DIR}/wc.so" "${ROOT_DIR}/data/pg/pg"*".txt" || exit 1
sort mr-out-0 > mr-correct-wc.txt
rm -f mr-out*

echo '***' Starting wc test.

maybe_quiet $TIMEOUT "${BIN_DIR}/mrcoordinator" "${COORDINATOR_ARGS[@]}" "${ROOT_DIR}/data/pg/pg"*".txt" &
pid=$!

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
(maybe_quiet $TIMEOUT "${BIN_DIR}/mrworker" "${WORKER_ARGS[@]}" -app="${PLUGIN_DIR}/wc.so") &
(maybe_quiet $TIMEOUT "${BIN_DIR}/mrworker" "${WORKER_ARGS[@]}" -app="${PLUGIN_DIR}/wc.so") &
(maybe_quiet $TIMEOUT "${BIN_DIR}/mrworker" "${WORKER_ARGS[@]}" -app="${PLUGIN_DIR}/wc.so") &

# wait for the coordinator to exit.
wait $pid

# since workers are required to exit when a job is completely finished,
# and not before, that means the job has finished.
OUTPUT_DIR=$(latest_output_dir || true)
if [ -n "$OUTPUT_DIR" ]; then
  sort "${OUTPUT_DIR}"/mr-out-* | grep . > mr-wc-all
fi
if [ -n "$OUTPUT_DIR" ] && cmp mr-wc-all mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi

# wait for remaining workers and coordinator to exit.
wait

#########################################################
# now indexer
rm -f mr-*

# generate the correct output
"${BIN_DIR}/mrsequential" "${PLUGIN_DIR}/indexer.so" "${ROOT_DIR}/data/pg/pg"*".txt" || exit 1
sort mr-out-0 > mr-correct-indexer.txt
rm -f mr-out*

echo '***' Starting indexer test.

maybe_quiet $TIMEOUT "${BIN_DIR}/mrcoordinator" "${COORDINATOR_ARGS[@]}" "${ROOT_DIR}/data/pg/pg"*".txt" &
sleep 1

# start multiple workers
maybe_quiet $TIMEOUT "${BIN_DIR}/mrworker" "${WORKER_ARGS[@]}" -app="${PLUGIN_DIR}/indexer.so" &
maybe_quiet $TIMEOUT "${BIN_DIR}/mrworker" "${WORKER_ARGS[@]}" -app="${PLUGIN_DIR}/indexer.so"

OUTPUT_DIR=$(latest_output_dir || true)
if [ -n "$OUTPUT_DIR" ]; then
  sort "${OUTPUT_DIR}"/mr-out-* | grep . > mr-indexer-all
fi
if [ -n "$OUTPUT_DIR" ] && cmp mr-indexer-all mr-correct-indexer.txt
then
  echo '---' indexer test: PASS
else
  echo '---' indexer output is not the same as mr-correct-indexer.txt
  echo '---' indexer test: FAIL
  failed_any=1
fi

wait

#########################################################
echo '***' Starting map parallelism test.

rm -f mr-*

maybe_quiet $TIMEOUT "${BIN_DIR}/mrcoordinator" "${COORDINATOR_ARGS[@]}" "${ROOT_DIR}/data/pg/pg"*".txt" &
sleep 1

maybe_quiet $TIMEOUT "${BIN_DIR}/mrworker" "${WORKER_ARGS[@]}" -app="${PLUGIN_DIR}/mtiming.so" &
maybe_quiet $TIMEOUT "${BIN_DIR}/mrworker" "${WORKER_ARGS[@]}" -app="${PLUGIN_DIR}/mtiming.so"

OUTPUT_DIR=$(latest_output_dir || true)
NT=0
if [ -n "$OUTPUT_DIR" ]; then
  NT=`cat "${OUTPUT_DIR}"/mr-out-* | grep '^times-' | wc -l | sed 's/ //g'`
fi
if [ "$NT" != "2" ]
then
  echo '---' saw "$NT" workers rather than 2
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

if [ -n "$OUTPUT_DIR" ] && cat "${OUTPUT_DIR}"/mr-out-* | grep '^parallel.* 2' > /dev/null
then
  echo '---' map parallelism test: PASS
else
  echo '---' map workers did not run in parallel
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

wait

#########################################################
echo '***' Starting reduce parallelism test.

rm -f mr-*

maybe_quiet $TIMEOUT "${BIN_DIR}/mrcoordinator" "${COORDINATOR_ARGS[@]}" "${ROOT_DIR}/data/pg/pg"*".txt" &
sleep 1

maybe_quiet $TIMEOUT "${BIN_DIR}/mrworker" "${WORKER_ARGS[@]}" -app="${PLUGIN_DIR}/rtiming.so"  &
maybe_quiet $TIMEOUT "${BIN_DIR}/mrworker" "${WORKER_ARGS[@]}" -app="${PLUGIN_DIR}/rtiming.so"

OUTPUT_DIR=$(latest_output_dir || true)
NT=0
if [ -n "$OUTPUT_DIR" ]; then
  NT=`cat "${OUTPUT_DIR}"/mr-out-* | grep '^[a-z] 2' | wc -l | sed 's/ //g'`
fi
if [ "$NT" -lt "2" ]
then
  echo '---' too few parallel reduces.
  echo '---' reduce parallelism test: FAIL
  failed_any=1
else
  echo '---' reduce parallelism test: PASS
fi

wait

#########################################################
echo '***' Starting job count test.

rm -f mr-*

maybe_quiet $TIMEOUT "${BIN_DIR}/mrcoordinator" "${COORDINATOR_ARGS[@]}" "${ROOT_DIR}/data/pg/pg"*".txt"  &
sleep 1

maybe_quiet $TIMEOUT "${BIN_DIR}/mrworker" "${WORKER_ARGS[@]}" -app="${PLUGIN_DIR}/jobcount.so" &
maybe_quiet $TIMEOUT "${BIN_DIR}/mrworker" "${WORKER_ARGS[@]}" -app="${PLUGIN_DIR}/jobcount.so"
maybe_quiet $TIMEOUT "${BIN_DIR}/mrworker" "${WORKER_ARGS[@]}" -app="${PLUGIN_DIR}/jobcount.so" &
maybe_quiet $TIMEOUT "${BIN_DIR}/mrworker" "${WORKER_ARGS[@]}" -app="${PLUGIN_DIR}/jobcount.so"

OUTPUT_DIR=$(latest_output_dir || true)
NT=0
if [ -n "$OUTPUT_DIR" ]; then
  NT=`cat "${OUTPUT_DIR}"/mr-out-* | awk '{print $2}'`
fi
if [ "$NT" -eq "8" ]
then
  echo '---' job count test: PASS
else
  echo '---' map jobs ran incorrect number of times "($NT != 8)"
  echo '---' job count test: FAIL
  failed_any=1
fi

wait

#########################################################
# early exit test
rm -f mr-*

echo '***' Starting early exit test.

DF=anydone$$
rm -f $DF

(maybe_quiet $TIMEOUT "${BIN_DIR}/mrcoordinator" "${COORDINATOR_ARGS[@]}" "${ROOT_DIR}/data/pg/pg"*".txt"; touch $DF) &

sleep 1

(maybe_quiet $TIMEOUT "${BIN_DIR}/mrworker" "${WORKER_ARGS[@]}" -app="${PLUGIN_DIR}/early_exit.so"; touch $DF) &
(maybe_quiet $TIMEOUT "${BIN_DIR}/mrworker" "${WORKER_ARGS[@]}" -app="${PLUGIN_DIR}/early_exit.so"; touch $DF) &
(maybe_quiet $TIMEOUT "${BIN_DIR}/mrworker" "${WORKER_ARGS[@]}" -app="${PLUGIN_DIR}/early_exit.so"; touch $DF) &

jobs &> /dev/null
if [[ "$OSTYPE" = "darwin"* ]]
then
  while [ ! -e $DF ]
  do
    sleep 0.2
  done
else
  wait -n
fi

rm -f $DF

OUTPUT_DIR=$(latest_output_dir || true)
if [ -n "$OUTPUT_DIR" ]; then
  sort "${OUTPUT_DIR}"/mr-out-* | grep . > mr-wc-all-initial
fi

wait

OUTPUT_DIR=$(latest_output_dir || true)
if [ -n "$OUTPUT_DIR" ]; then
  sort "${OUTPUT_DIR}"/mr-out-* | grep . > mr-wc-all-final
fi
if [ -n "$OUTPUT_DIR" ] && cmp mr-wc-all-final mr-wc-all-initial
then
  echo '---' early exit test: PASS
else
  echo '---' output changed after first worker exited
  echo '---' early exit test: FAIL
  failed_any=1
fi
rm -f mr-*

#########################################################
echo '***' Starting crash test.

# generate the correct output
"${BIN_DIR}/mrsequential" "${PLUGIN_DIR}/nocrash.so" "${ROOT_DIR}/data/pg/pg"*".txt" || exit 1
sort mr-out-0 > mr-correct-crash.txt
rm -f mr-out*

rm -f mr-done
((maybe_quiet $TIMEOUT2 "${BIN_DIR}/mrcoordinator" "${COORDINATOR_ARGS[@]}" "${ROOT_DIR}/data/pg/pg"*".txt"); touch mr-done ) &
sleep 1

maybe_quiet $TIMEOUT2 "${BIN_DIR}/mrworker" "${WORKER_ARGS[@]}" -app="${PLUGIN_DIR}/crash.so" &

( while [ ! -f mr-done ]
  do
    maybe_quiet $TIMEOUT2 "${BIN_DIR}/mrworker" "${WORKER_ARGS[@]}" -app="${PLUGIN_DIR}/crash.so"
    sleep 1
  done ) &

( while [ ! -f mr-done ]
  do
    maybe_quiet $TIMEOUT2 "${BIN_DIR}/mrworker" "${WORKER_ARGS[@]}" -app="${PLUGIN_DIR}/crash.so"
    sleep 1
  done ) &

while [ ! -f mr-done ]
do
  maybe_quiet $TIMEOUT2 "${BIN_DIR}/mrworker" "${WORKER_ARGS[@]}" -app="${PLUGIN_DIR}/crash.so"
  sleep 1
done

wait

OUTPUT_DIR=$(latest_output_dir || true)
if [ -n "$OUTPUT_DIR" ]; then
  sort "${OUTPUT_DIR}"/mr-out-* | grep . > mr-crash-all
fi
if [ -n "$OUTPUT_DIR" ] && cmp mr-crash-all mr-correct-crash.txt
then
  echo '---' crash test: PASS
else
  echo '---' crash output is not the same as mr-correct-crash.txt
  echo '---' crash test: FAIL
  failed_any=1
fi

#########################################################
if [ $failed_any -eq 0 ]; then
    echo '***' PASSED ALL TESTS
else
    echo '***' FAILED SOME TESTS
    exit 1
fi
