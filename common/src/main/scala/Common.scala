package osmdiff

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession


object Common {

  def sparkSession(appName: String): SparkSession = {
    val conf = new SparkConf()
      .setAppName(appName)
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.executor.heartbeatInterval", "30")
      .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .set("spark.hadoop.hive.execution.engine", "spark")
      .set("spark.hadoop.hive.vectorized.execution.enabled", "true")
      .set("spark.hadoop.hive.vectorized.execution.reduce.enabled", "true")
      .set("spark.sql.hive.metastorePartitionPruning", "true")
      .set("spark.sql.orc.filterPushdown", "true")

    SparkSession.builder
      .config(conf)
      .enableHiveSupport
      .getOrCreate
  }

  val getInstant = udf({ (ts: java.sql.Timestamp) => ts.getTime })

  def transitiveClosure(osm: DataFrame, existingIndex: Option[DataFrame]): DataFrame = {
      // Beginnings of transitive chains
      val nodeToWays = osm
        .filter(col("type") === "way")
        .select(
        explode(col("nds.ref")).as("from_id"),
          Common.getInstant(col("timestamp")).as("instant"),
          col("id").as("to_id"),
          col("type").as("to_type"))
        .withColumn("from_type", lit("node"))
      val xToRelations = osm
        .filter(col("type") === "relation")
        .select(
        explode(col("members")).as("from"),
          Common.getInstant(col("timestamp")).as("instant"),
          col("id").as("to_id"),
          col("type").as("to_type"))
        .withColumn("from_id", col("from.ref"))
        .withColumn("from_type", col("from.type"))
        .drop("from")

      // Compute transitive chains
      var index = nodeToWays.union(xToRelations).distinct
      var additions: DataFrame = null
      do {
        val joined = existingIndex match {
          case Some(existingIndex) => {
            index.as("left")
              .join(
              index.union(existingIndex).as("right"),
                ((col("left.to_id") === col("right.from_id")) &&
                 (col("left.to_type") === col("right.from_type")) &&
                 (col("left.instant") <= col("right.instant"))), // XXX backwards?
                "inner")
          }
          case None => {
            index.as("left")
              .join(
              index.as("right"),
                ((col("left.to_id") === col("right.from_id")) &&
                 (col("left.to_type") === col("right.from_type")) &&
                 (col("left.instant") <= col("right.instant"))), // XXX
                "inner")
          }
        }
        additions = joined
          .select(
            col("left.from_id").as("from_id"),
            col("left.from_type").as("from_type"),
            col("right.to_id").as("to_id"),
            col("right.to_type").as("to_type"),
            col("right.instant").as("instant"))
        index = index.union(additions).distinct
        println("...")
      } while(additions.count > 0)

      // Return index
      index
  }

}
