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

  def partitionNumberFn(id: Long, tipe: String): Long = {
    var a = id
    while (a > ((1L)<<(12-1))) {
      a = a/ 10
    }
    val b = tipe match {
      case "node" => 0L
      case "way" => 1L
      case "relation" => 2L
    }
    a ^ b
  }

  val partitionNumberUdf = udf({ (id: Long, tipe: String) =>
    partitionNumberFn(id, tipe)
  })

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
    col("ap"),
    col("a"),
    col("instant"),
    col("bp"),
    col("b"),
    col("iteration"),
    col("extra"))

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
      .orderBy("ap", "a")
      .write
      .mode(mode)
      .format("orc")
      .partitionBy("ap")
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
          partitionNumberUdf(col("nds.ref"), lit("node")).as("ap"),
          struct(col("nds.ref").as("id"), lit("node").as("type")).as("a"),
          col("instant"),
          partitionNumberUdf(col("b.id"), col("b.type")).as("bp"),
          col("b"),
          lit(0L).as("iteration"),
          lit(false).as("extra"))
    val halfEdgesFromRelations =
      rows
        .filter(col("type") === "relation")
        .select(
          struct(col("id").as("id"), col("type").as("type")).as("b"),
          Common.getInstant(col("timestamp")).as("instant"),
          explode(col("members")).as("members"))
        .select(
          partitionNumberUdf(col("members.ref"), col("members.type")).as("ap"),
          struct(col("members.ref").as("id"), col("members.type").as("type")).as("a"),
          col("instant"),
          partitionNumberUdf(col("b.id"), col("b.type")).as("bp"),
          col("b"),
          lit(0L).as("iteration"),
          lit(false).as("extra"))
    val halfEdges = halfEdgesFromNodes.union(halfEdgesFromRelations)

    // Return new edges
    halfEdges
  }

  def transitiveStep(oldEdges: DataFrame, newEdges: DataFrame, iteration: Long): DataFrame = {
    logger.info(s"Transitive closure iteration $iteration")
    oldEdges
      .filter((col("iteration") === iteration-1) && (col("extra") === false))
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
        col("left.ap").as("ap"),
        col("left.a").as("a"),
        Common.larger(col("left.instant"), col("right.instant")).as("instant"),
        col("right.bp").as("bp"),
        col("right.b").as("b"),
        lit(iteration).as("iteration"),
        lit(false).as("extra"))
  }

  def reset(edges: DataFrame): DataFrame = { // XXX optimization barrier question
    edges
      .select(
        col("ap"),
        col("a"),
        col("instant"),
        col("bp"),
        col("b"),
        lit(0L).as("iteration"),
        col("extra"))
  }

  def mirror(edges: DataFrame): DataFrame = {
    edges
      .select(
        col("bp").as("ap"),
        col("b").as("a"),
        col("instant"),
        col("ap").as("bp"),
        col("a").as("b"),
        lit(0L).as("iteration"),
        lit(true).as("extra"))
  }

  def transitiveClosure(rows: DataFrame, oldEdgesOption: Option[DataFrame]): DataFrame = {
    val newEdges = edgesFromRows(rows).select(edgeColumns: _*).cache
    logger.info(s"Transitive closure iteration 0")
    // logger.info(s"Additions: ${newEdges.count}")

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
      val additions = transitiveStep(oldEdges, newEdges, iteration).select(edgeColumns: _*).cache
      try {
        additions.head
        // logger.info(s"Additions: ${additions.count}")
        oldEdges = oldEdges.union(additions).select(edgeColumns: _*)
        allAdditions = allAdditions.union(additions).select(edgeColumns: _*)
        keepGoing = true
        iteration = iteration + 1L
      } catch {
        case e: Exception => keepGoing = false
      }
    } while (keepGoing)

    reset(allAdditions).select(edgeColumns: _*)
      .union(mirror(allAdditions).select(edgeColumns: _*))
  }

}
