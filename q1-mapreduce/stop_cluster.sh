#!/usr/bin/env bash
# Stops the HDFS and YARN daemons started by run_cluster.sh
export HADOOP_HOME="${HADOOP_HOME:-$(brew --prefix hadoop)/libexec}"
export HADOOP_CONF_DIR="$(cd "$(dirname "$0")" && pwd)/hadoop-config"
"$HADOOP_HOME/bin/yarn" --daemon stop nodemanager || true
"$HADOOP_HOME/bin/yarn" --daemon stop resourcemanager || true
"$HADOOP_HOME/bin/hdfs" --daemon stop datanode || true
"$HADOOP_HOME/bin/hdfs" --daemon stop namenode || true
