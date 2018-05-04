#!/bin/sh

set -x

/opt/start-hdfs.sh
sleep 5s

schematool -dbType postgres -initSchema
echo "ALTER TABLE \"TBLS\" ALTER COLUMN \"IS_REWRITE_ENABLED\" DROP NOT NULL;" | PGPASSWORD=hive psql -h metastore-database -U hive -d metastore
