#!/usr/bin/env bash
# Runs the keyword search with Hadoop in local mode, no daemons needed.
# Usage: ./run_local.sh [keyword]
# Leave the keyword out and the program will ask for it.
set -euo pipefail
cd "$(dirname "$0")"

INPUT=../data/extracted/map_reduce_test_docs
OUTPUT=output/local

mvn -q -B package -DskipTests
rm -rf "$OUTPUT"

hadoop jar target/keyword-search.jar \
  -D fs.defaultFS=file:/// \
  -D mapreduce.framework.name=local \
  "$INPUT" "$OUTPUT" "$@"

echo
echo "Result from $OUTPUT/part-r-00000"
cat "$OUTPUT/part-r-00000"
