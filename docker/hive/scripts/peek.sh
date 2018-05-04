#!/bin/sh

set -x

/opt/start-hdfs.sh
sleep 5s

schematool -dbType postgres -info
