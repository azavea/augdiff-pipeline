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
      .setIfMissing("spark.executor.heartbeatInterval", "30")
      .setIfMissing("spark.hadoop.hive.execution.engine", "spark")
      .setIfMissing("spark.master", "local[*]")
      .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .set("spark.hadoop.hive.vectorized.execution.enabled", "true")
      .set("spark.hadoop.hive.vectorized.execution.reduce.enabled", "true")
      .set("spark.sql.hive.metastorePartitionPruning", "true")
      .set("spark.sql.orc.filterPushdown", "true")

    SparkSession.builder
      .config(conf)
      .enableHiveSupport
      .getOrCreate
  }

  val larger = udf({ (x: Long, y: Long) => math.max(x,y) })

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

  val edgeColumns: List[Column] = List(
    col("a"),
    col("instant"),
    col("b"),
    col("iteration"))

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
      .partitionBy("x")
    .saveAsTable(tableName)
  }

  def edgesFromRows(rows: DataFrame): DataFrame = {
    val halfEdgesFromNodes =
      rows
        .filter(col("type") === "way")
        .select(
          struct(col("id").as("id"), col("type").as("type")).as("b"),
          Common.getInstant(col("timestamp")).as("instant"),
          explode(col("nds")).as("nds"))
        .select(
          struct(col("nds.ref").as("id"), lit("node").as("type")).as("a"),
          col("instant"),
          col("b"),
          lit(0L).as("iteration"))
    val halfEdgesFromRelations =
      rows
        .filter(col("type") === "relation")
        .select(
          struct(col("id").as("id"), col("type").as("type")).as("b"),
          Common.getInstant(col("timestamp")).as("instant"),
          explode(col("members")).as("members"))
        .select(
          struct(col("members.ref").as("id"), col("members.type").as("type")).as("a"),
          col("instant"),
          col("b"),
          lit(0L).as("iteration"))
    val halfEdges = halfEdgesFromNodes.union(halfEdgesFromRelations)

    // Return new edges
    halfEdges
  }

  def transitiveStep(oldEdges: DataFrame, newEdges: DataFrame, iteration: Long): DataFrame = {
    logger.info(s"Transitive closure iteration $iteration")
    oldEdges
      .filter(col("iteration") === iteration-1)
      .as("left")
      .join(
        newEdges.as("right"),
        ((col("left.b") === col("right.a")) && // The two edges meet
         (col("left.a.type") =!= lit("way") || col("right.b.type") =!= lit("way")) && // Do no join way to way
         (col("left.a.type") =!= lit("node") || col("right.b.type") =!= lit("node")) && // Do no join node to node
         (col("left.a") =!= col("right.b")) // Do not join something to itself
        ),
        "inner")
      .select(
        col("left.a").as("a"),
        Common.larger(col("left.instant"), col("right.instant")).as("instant"),
        col("right.b").as("b"),
        lit(iteration).as("iteration"))
  }

  def transitiveClosure(rows: DataFrame, oldEdgesOption: Option[DataFrame]): DataFrame = {
    logger.info(s"Transitive closure iteration 0")

    val newEdges = edgesFromRows(rows).select(edgeColumns: _*)
    var allAdditions = newEdges
    var oldEdges = (oldEdgesOption match {
      case Some(edges) =>
        edges
          .select(edgeColumns: _*)
          .union(newEdges)
      case None => newEdges
    })
      .select(edgeColumns: _*)

    var iteration = 1L
    var keepGoing = false
    do {
      val additions = transitiveStep(oldEdges, newEdges, iteration).select(edgeColumns: _*)
      try {
        additions.head
        // logger.info(s"ADDITIONS: ${additions.count}")
        additions.show // XXX
        oldEdges = oldEdges.union(additions).select(edgeColumns: _*)
        allAdditions = allAdditions.union(additions).select(edgeColumns: _*)
        keepGoing = true
        iteration = iteration + 1L
      } catch {
        case e: Exception => keepGoing = false
      }
    } while (keepGoing)

    allAdditions
  }

}
