#!/bin/sh

set -x

/opt/start-hdfs.sh
sleep 5s

schematool -dbType postgres -initSchema
echo "ALTER TABLE \"TBLS\" ALTER COLUMN \"IS_REWRITE_ENABLED\" DROP NOT NULL;" | PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DB
