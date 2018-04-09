package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object Indexer {

  def main(args: Array[String]): Unit = {
    val spark = Common.sparkSession("Indexer")
    import spark.implicits._

    val osm = spark.read.orc(args(0))

    val nodeToWays = osm
      .filter(col("type") === "way")
      .select(
        explode(col("nds.ref")).as("from_id"),
        Common.getInstant(col("timestamp")).as("instant"),
        col("id").as("to_id"),
        col("type").as("to_type"))
      .withColumn("from_type", lit("node"))
    .distinct
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

    osm
      .write
      .mode("overwrite")
      .format("orc")
      .sortBy("id", "type", "timestamp").bucketBy(1, "id", "type")
      .saveAsTable("osm")
    nodeToWays.union(xToRelations)
      .write
      .mode("overwrite")
      .format("orc")
      .sortBy("from_id", "from_type", "instant").bucketBy(1, "from_id", "from_type")
      .saveAsTable("index")
  }

}
