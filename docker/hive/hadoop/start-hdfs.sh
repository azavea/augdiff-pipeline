#!/bin/sh
set -ex

echo "Y" | hdfs namenode -format
nohup hdfs namenode &
nohup hdfs secondarynamenode &
nohup hdfs datanode &
exec "$@"
