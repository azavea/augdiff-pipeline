package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


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

  val ndsSchema = ArrayType(StructType(List(StructField("ref", LongType, true))))
  val membersSchema = ArrayType(StructType(List(
    StructField("type", StringType, true),
    StructField("ref", LongType, true),
    StructField("role", StringType, true))))
  val osmSchema = StructType(List(
    StructField("id", LongType, true),
    StructField("type", StringType, true),
    StructField("tags", MapType(StringType, StringType), true),
    StructField("lat", DecimalType(9, 7), true),
    StructField("lon", DecimalType(10, 7), true),
    StructField("nds", ndsSchema, true),
    StructField("members", membersSchema, true),
    StructField("changeset", LongType, true),
    StructField("timestamp", TimestampType, true),
    StructField("uid", LongType, true),
    StructField("user", StringType, true),
    StructField("version", LongType, true),
    StructField("visible", BooleanType, true)))

  val osmColumns: List[Column] = List(
    col("id"),
    col("type"),
    col("tags"),
    col("lat"),
    col("lon"),
    col("nds"),
    col("members"),
    col("changeset"),
    col("timestamp"),
    col("uid"),
    col("user"),
    col("version"),
    col("visible"))

  val indexColumns: List[Column] = List(
    col("prior_id"),
    col("prior_type"),
    col("instant"),
    col("dependent_id"),
    col("dependent_type"))
  val indexColumns2: List[Column] = indexColumns ++ List(col("iteration"))

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
    logger.info(s"Writing bulk")
    bulk
      .write
      .mode(mode)
      .format("orc")
      .sortBy("id", "type", "timestamp").bucketBy(1, "id", "type")
      .saveAsTable(tableName)
  }

  def saveIndex(index: DataFrame, tableName: String, mode: String): Unit = {
    logger.info(s"Writing index")
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
    var indexUpdates = nodeToWays.select(Common.indexColumns: _*)
      .union(xToRelations.select(Common.indexColumns: _*))
      .withColumn("iteration", lit(0L))
      .distinct // XXX
    val index: DataFrame = existingIndex match {
      case Some(existingIndex) =>
        existingIndex.select(Common.indexColumns: _*)
          .union(indexUpdates.select(Common.indexColumns: _*))
      case None => indexUpdates
    }
    var i: Long = 1; var keepGoing = true
    do {
      logger.info(s"Transitive closure iteration $i")
      val additions = index.as("left") // somewhat overkill
        .join(
          indexUpdates.filter(col("iteration") === (i-1)).as("right"),
          ((col("left.dependent_id") === col("right.prior_id")) &&
           (col("left.dependent_type") === col("right.prior_type")) &&
           (col("left.instant") <= col("right.instant"))), // XXX <=
          "inner")
        .select(
          col("left.prior_id").as("prior_id"),
          col("left.prior_type").as("prior_type"),
          col("right.dependent_id").as("dependent_id"),
          col("right.dependent_type").as("dependent_type"),
          col("left.instant").as("instant"),  // XXX instant?
          lit(i).as("iteration"))
        .filter(!(col("prior_id") === col("dependent_id") && col("prior_type") === col("dependent_type")))
      try {
        additions.head
        keepGoing = true
      } catch {
        case e: Exception => keepGoing = false
      }
      indexUpdates =
        indexUpdates.select(Common.indexColumns2: _*)
          .union(additions.select(Common.indexColumns2: _*))
          .distinct // XXX
      i=i+1
    } while(keepGoing)

    // Return index
    indexUpdates
  }

}
