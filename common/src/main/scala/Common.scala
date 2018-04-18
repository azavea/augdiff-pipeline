package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import scala.collection.mutable


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

  val pfLimit = 150 // Partition filter size limit
  val idLimit = 4096 // Predicate pushdown size limit

  private val bits = 16

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
    col("p"),         /* 0 */
    col("id"),        /* 1 */
    col("type"),      /* 2 */
    col("tags"),      /* 3 */
    col("lat"),       /* 4 */
    col("lon"),       /* 5 */
    col("nds"),       /* 6 */
    col("members"),   /* 7 */
    col("changeset"), /* 8 */
    col("timestamp"), /* 9 */
    col("uid"),       /* 10 */
    col("user"),      /* 11 */
    col("version"),   /* 12 */
    col("visible"))   /* 13 */

  val edgeColumns: List[Column] = List(
    col("ap"), col("aid"), col("atype"), /* 0, 1, 2 */
    col("instant"),                      /* 3 */
    col("bp"), col("bid"), col("btype"), /* 4, 5, 6 */
    col("a_to_b"))                       /* 7 */

  val edgeColumnsPlus: List[Column] = edgeColumns :+ col("iteration") /* 8 */

  private val logger = {
    val logger = Logger.getLogger(this.getClass)
    logger.setLevel(Level.INFO)
    logger
  }

  def denoise(): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
  }

  def loadEdges(desired: Set[(Long, String)], edges: DataFrame): mutable.Set[Row] = { // XXX too many instants
    val pairs = desired.groupBy({ pair => partitionNumberFn(pair._1, pair._2) })
    logger.info(s"◼ Reading ${pairs.size} partitions in groups of ${pfLimit}") // 175 bug (pfLimit <= 175)
    val dfs = pairs.grouped(pfLimit).map({ _group =>
      logger.info("◼ Reading group")
      val group = _group.toArray
      val ps = group.map({ kv => kv._1 })
      val ids = group.flatMap({ kv => kv._2.map(_._1) }).distinct
      val retval = edges.filter(col("bp").isin(ps: _*)) // partition pruning
      if (ids.length < idLimit)
        retval.filter(col("bid").isin(ids: _*)) // predicate pushdown
      else retval
    })
    val s = mutable.Set.empty[Row]
    dfs.foreach({ df =>
      s ++= df.select(edgeColumns: _*)
        .collect
        .filter({ r => desired.contains((r.getLong(1) /* aid */, r.getString(2) /* atype */)) })
    })
    s
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

}
