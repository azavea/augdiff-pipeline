package osmdiff

import org.apache.log4j.{Level, Logger}
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

  private val logger = {
    val logger = Logger.getLogger(Common.getClass)
    logger.setLevel(Level.INFO)
    logger
  }

  def denoise(): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
  }

  def saveBulk(bulk: DataFrame, tableName: String, mode: String): Unit = {
    bulk
      .write
      .mode(mode)
      .format("orc")
      .sortBy("id", "type", "timestamp").bucketBy(1, "id", "type")
      .saveAsTable(tableName)
  }

  def saveIndex(index: DataFrame, tableName: String, mode: String): Unit = {
    index
      .write
      .mode(mode)
      .format("orc")
      .sortBy("prior_id", "prior_type", "instant").bucketBy(1, "prior_id", "prior_type")
      .saveAsTable(tableName)
  }

  def transitiveClosure(osm: DataFrame, existingIndex: Option[DataFrame]): DataFrame = {
    // Beginnings of transitive chains
    val nodeToWays = osm
      .filter(col("type") === "way")
      .select(
      explode(col("nds.ref")).as("prior_id"),
        Common.getInstant(col("timestamp")).as("instant"),
        col("id").as("dependent_id"),
        col("type").as("dependent_type"))
      .withColumn("prior_type", lit("node"))
    val xToRelations = osm
      .filter(col("type") === "relation")
      .select(
      explode(col("members")).as("prior"),
        Common.getInstant(col("timestamp")).as("instant"),
        col("id").as("dependent_id"),
        col("type").as("dependent_type"))
      .withColumn("prior_id", col("prior.ref"))
      .withColumn("prior_type", col("prior.type"))
      .drop("prior")

    // Compute transitive chains
    var indexUpdates = nodeToWays.union(xToRelations).distinct // XXX
    val index: DataFrame = existingIndex match {
      case Some(existingIndex) => existingIndex.union(indexUpdates)
      case None => indexUpdates
    }
    var keepGoing = true
    do {
      val additions = index.as("left") // somewhat overkill
        .join(
          indexUpdates.as("right"),
          ((col("left.dependent_id") === col("right.prior_id")) &&
           (col("left.dependent_type") === col("right.prior_type")) &&
           (col("left.instant") <= col("right.instant"))),
          "inner")
        .select(
          col("left.prior_id").as("prior_id"),
          col("left.prior_type").as("prior_type"),
          col("right.dependent_id").as("dependent_id"),
          col("right.dependent_type").as("dependent_type"),
          col("right.instant").as("instant"))
      indexUpdates = indexUpdates.union(additions).distinct // XXX
      keepGoing = !additions.rdd.isEmpty
      logger.info("transitive closure iteration")
    } while(keepGoing)

    // Return index
    indexUpdates
  }

}
