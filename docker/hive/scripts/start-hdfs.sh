#!/bin/sh

set -x

echo "Y" | hdfs namenode -format
nohup hdfs namenode &> /dev/null &
nohup hdfs secondarynamenode &> /dev/null &
nohup hdfs datanode &> /dev/null &
