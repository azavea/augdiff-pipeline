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
