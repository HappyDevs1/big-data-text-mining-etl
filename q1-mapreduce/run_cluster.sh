#!/usr/bin/env bash
# Runs the keyword search on a single node HDFS and YARN cluster.
# Usage: ./run_cluster.sh [keyword]
# Uses the XML files in hadoop-config. Daemons are started with --daemon so no ssh setup is needed.
set -euo pipefail
cd "$(dirname "$0")"

export HADOOP_HOME="${HADOOP_HOME:-$(brew --prefix hadoop)/libexec}"
export HADOOP_CONF_DIR="$PWD/hadoop-config"
export HADOOP_MAPRED_HOME="$HADOOP_HOME"
export JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home 2>/dev/null || true)}"

HDFS_INPUT=/user/$USER/keyword-search/input
HDFS_OUTPUT=/user/$USER/keyword-search/output

mvn -q -B package -DskipTests

# Format HDFS the first time only
if [ ! -d "/tmp/hadoop-$USER/dfs/name/current" ]; then
  "$HADOOP_HOME/bin/hdfs" namenode -format -nonInteractive
fi

"$HADOOP_HOME/bin/hdfs" --daemon start namenode || echo "already running"
"$HADOOP_HOME/bin/hdfs" --daemon start datanode || echo "already running"
"$HADOOP_HOME/bin/yarn" --daemon start resourcemanager || echo "already running"
"$HADOOP_HOME/bin/yarn" --daemon start nodemanager || echo "already running"
"$HADOOP_HOME/bin/hdfs" dfsadmin -safemode wait

"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p "$HDFS_INPUT"
"$HADOOP_HOME/bin/hdfs" dfs -put -f ../data/extracted/map_reduce_test_docs/*.txt "$HDFS_INPUT"
"$HADOOP_HOME/bin/hdfs" dfs -ls "$HDFS_INPUT"

"$HADOOP_HOME/bin/hadoop" jar target/keyword-search.jar -D search.overwrite=true "$HDFS_INPUT" "$HDFS_OUTPUT" "$@"

echo
echo "Result from HDFS"
"$HADOOP_HOME/bin/hdfs" dfs -cat "$HDFS_OUTPUT/part-r-00000"

mkdir -p output/cluster
"$HADOOP_HOME/bin/hdfs" dfs -get -f "$HDFS_OUTPUT/part-r-00000" output/cluster/
