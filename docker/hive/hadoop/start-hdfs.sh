#!/bin/sh
set -e

echo "Y" | hdfs namenode -format
nohup hdfs namenode &
nohup hdfs secondarynamenode &
nohup hdfs datanode &

