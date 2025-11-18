#!/usr/bin/env bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 numTrials"
    exit 1
fi

runs=$1

trap 'kill -INT -$pid; exit 1' INT

chmod +x test-mr.sh

# Detect timeout / gtimeout (similar to test-mr.sh)
TIMEOUT=timeout
if timeout 2s sleep 1 > /dev/null 2>&1; then
  :
else
  if gtimeout 2s sleep 1 > /dev/null 2>&1; then
    TIMEOUT=gtimeout
  else
    TIMEOUT=""
    echo '*** Cannot find timeout command; proceeding without timeouts.'
  fi
fi

for i in $(seq 1 "$runs"); do
    if [ -n "$TIMEOUT" ]; then
        $TIMEOUT -k 2s 900s ./test-mr.sh quiet &
    else
        ./test-mr.sh quiet &
    fi
    pid=$!
    if ! wait $pid; then
        echo '***' FAILED TESTS IN TRIAL "$i"
        exit 1
    fi
done

echo '***' PASSED ALL "$i" TESTING TRIALS