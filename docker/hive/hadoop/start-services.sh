#!/bin/sh
set -e

echo "Y" | hdfs namenode -format
nohup hdfs namenode &> /dev/null &
nohup hdfs secondarynamenode &> /dev/null &
nohup hdfs datanode &> /dev/null &
nohup $HIVE_HOME/bin/hiveserver2 &> /dev/null &
