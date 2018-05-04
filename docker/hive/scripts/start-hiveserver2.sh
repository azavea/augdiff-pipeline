#!/bin/sh

set -x

/opt/start-hdfs.sh
nohup hiveserver2
