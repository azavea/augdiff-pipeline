# How To Run #

## Indexer ##

The indexer takes an ORC file containing the starting state and produces hive tables necessary for computing augmented diffs.

Because the indexer creates hive tables in the directory in which it is run, it is recommended that it be run from some directory outside of the source tree.

To index an ORC file, type something like
```bash
$SPARK_HOME/bin/spark-submit --driver-memory 16G ~/local/src/indexer/target/scala-2.11/indexer.jar ./area_of_interest.orc
```

## Augmented Differ ##

The augmented differ takes an OSC file containing change information and updates the hive tables produced by the indexer.

To apply change information, type something like
```bash
$SPARK_HOME/bin/spark-submit --driver-memory 16G ~/local/src/ad/target/scala-2.11/ad.jar ./area_of_interest.osc
```

## Postgres Metastore ##

Successfully running the postgres metastore should be relatively
pain-free with the help of `docker-compose`. First, we'll need to bring
up a fresh image of postgres. In preparation for that, make sure
`data/pgdata` is empty (this is where we'll mount our PG database to
persist tables when the image is not running). Then, run
```bash
docker-compose up metastore-database
```

Once that's up, we'll need to set up the schemas to match Hive's
expectations:
```bash
docker-compose build
docker-compose up metastore-init
```

The purpose of the `docker-compose build` line above is to build the `hive` image from-which the metastore initialization takes place.
If the `hive` image already exists, then that line can be safely omitted.

Upon successful provisioning of the database, the `metastore-init`
container should shut itself down. To verify that the tables are ready
for use, simply run
```bash
docker-compose up metastore-info
```
to print schema version information.

Next, type
```bash
docker-compose exec metastore-database bash -c 'echo "ALTER TABLE \"TBLS\" ALTER COLUMN \"IS_REWRITE_ENABLED\" DROP NOT NULL;" | psql -d metastore -U hive'
```
to apply a work-around to the postgres database.
The statement
```sql
ALTER TABLE "TBLS" ALTER COLUMN "IS_REWRITE_ENABLED" DROP NOT NULL;
```
makes the schema of the table `TBLS` compatible with the version of hive that is packaged with Spark 2.3.

To actually use Hive, Hadoop, and Spark in conjunction with the external
metastore, simply run
```bash
docker-compose run hive bash
```
to bring up a terminal in an environment where spark's hive
functionality will be configured to work against the external metastore

From inside of the container, hive can be interacted with by e.g. typing
```bash
/opt/start-hdfs.sh
spark-shell --conf spark.sql.warehouse.dir=file:///tmp --conf spark.hadoop.hive.metastore.warehouse.dir=file:///tmp --conf spark.jars=file:///usr/share/java/postgresql-jdbc4.jar
```
where the first line starts HDFS and the second line starts a `spark-shell` capabile of interacting with hive.
