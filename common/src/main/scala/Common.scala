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
      .setIfMissing("spark.ui.enabled", "true")
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

  private val bits = 12

  def partitionNumberFn(id: Long, tipe: String): Long = {
    var a = id
    while (a > ((1L)<<(bits-1))) {
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
    StructField("p", LongType, true),
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
    col("p"),
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
    col("ap"), col("aid"), col("atype"),
    col("instant"),
    col("bp"), col("bid"), col("btype"),
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
      .orderBy("p", "id", "type")
      .write
      .mode(mode)
      .format("orc")
      .partitionBy("p")
      .saveAsTable(tableName)
  }

  def saveIndex(index: DataFrame, tableName: String, mode: String): Unit = {
    logger.info(s"Writing index")
    index
      .orderBy("bp", "bid", "btype")
      .write
      .mode(mode)
      .format("orc")
      .partitionBy("bp")
    .saveAsTable(tableName)
  }

  def edgesFromRows(rows: DataFrame): DataFrame = {
    val halfEdgesFromNodes =
      rows
        .filter(col("type") === "way")
        .select(
          col("id").as("bid"),
          col("type").as("btype"),
          Common.getInstant(col("timestamp")).as("instant"),
          explode(col("nds")).as("nds"))
        .select(
          partitionNumberUdf(col("nds.ref"), lit("node")).as("ap"),
          col("nds.ref").as("aid"),
          lit("node").as("atype"),
          col("instant"),
          partitionNumberUdf(col("bid"), col("btype")).as("bp"),
          col("bid"),
          col("btype"),
          lit(0L).as("iteration"),
          lit(false).as("extra"))
    val halfEdgesFromRelations =
      rows
        .filter(col("type") === "relation")
        .select(
          col("id").as("bid"),
          col("type").as("btype"),
          Common.getInstant(col("timestamp")).as("instant"),
          explode(col("members")).as("members"))
        .select(
          partitionNumberUdf(col("members.ref"), col("members.type")).as("ap"),
          col("members.ref").as("aid"),
          col("members.type").as("atype"),
          col("instant"),
          partitionNumberUdf(col("bid"), col("btype")).as("bp"),
          col("bid"),
          col("btype"),
          lit(0L).as("iteration"),
          lit(false).as("extra"))
    val halfEdges = halfEdgesFromNodes.union(halfEdgesFromRelations)

    // Return new edges
    halfEdges
  }

  // def transitiveStepSomewhatLessSlow(
  //   leftEdges: DataFrame,
  //   rightEdges: Map[(Long, String), Array[Row]],
  //   partitions: Array[Long],
  //   iteration: Long
  // ): DataFrame = {
  //   logger.info(s"Transitive closure iteration $iteration (somewhat less slow)")
  //   val schema = leftEdges.schema
  //   val rdd = leftEdges
  //     // .filter(col("bp").isin(partitions: _*)) // Use partition pruning
  //     .filter(col("bp").isin((partitions.take(175)): _*)) // XXX work around limit w/ union
  //     .filter((col("iteration") === iteration-1) && (col("extra") === false))
  //     .select((edgeColumns.take(5)): _*).rdd
  //     .flatMap({ row1 => // Manual inner join
  //       val leftAp = row1.getLong(0)
  //       val leftA = row1.getStruct(1)
  //       val leftAid = leftA.getLong(0)
  //       val leftAtype = leftA.getString(1)
  //       val leftInstant = row1.getLong(2)
  //       val leftBp = row1.getLong(3)
  //       val leftB = row1.getStruct(4)
  //       val leftBid = leftB.getLong(0)
  //       val leftBtype = leftB.getString(1)
  //       val key = (leftB.getLong(0), leftB.getString(1))

  //       rightEdges.getOrElse(key, Array.empty[Row])
  //         .flatMap({ row2 =>
  //           val rightAp = row2.getLong(0)
  //           val rightA = row2.getStruct(1)
  //           val rightAid = rightA.getLong(0)
  //           val rightAtype = rightA.getString(1)
  //           val rightInstant = row2.getLong(2)
  //           val rightBp = row2.getLong(3)
  //           val rightB = row2.getStruct(4)
  //           val rightBid = rightB.getLong(0)
  //           val rightBtype = rightB.getString(1)

  //           if (leftBid != rightAid || leftBtype != rightAtype) None // The two edges must meet
  //           else if (leftAtype == "way" && rightBtype == "way") None // Do not join way to way
  //           else if (leftAtype == "node" && rightBtype == "node") None // Do not join node to node
  //           else if (leftAid == rightBid && leftAtype == rightBtype) None // Do not join thing to itself
  //           else {
  //             Some(Row(
  //               leftAp,
  //               leftA,
  //               math.max(leftInstant, rightInstant),
  //               rightBp,
  //               rightB,
  //               iteration,
  //               false
  //             ))
  //           }
  //         })
  //     })

  //   leftEdges.sparkSession.createDataFrame(rdd, schema)
  // }
}
