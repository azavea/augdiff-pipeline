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

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val osm = spark.read.orc(args(0))
    val index = {
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
        additions = index.as("left")
          .join(
          index.as("right"),
            ((col("left.to_id") === col("right.from_id")) &&
             (col("left.to_type") === col("right.from_type")) &&
             (col("left.instant") <= col("right.instant"))), // XXX backwards?
            "inner")
          .select(
            col("left.from_id").as("from_id"),
            col("left.from_type").as("from_type"),
            col("right.to_id").as("to_id"),
            col("right.to_type").as("to_type"),
            col("right.instant").as("instant"))
        index = index.union(additions).distinct
        println(s"XXX ${additions.count} ${index.count}")
      } while(additions.count > 0)

      // Return index
      index
    }

    osm
      .write
      .mode("overwrite")
      .format("orc")
      .sortBy("id", "type", "timestamp").bucketBy(1, "id", "type")
      .saveAsTable("osm")
    index
      .write
      .mode("overwrite")
      .format("orc")
      .sortBy("from_id", "from_type", "instant").bucketBy(1, "from_id", "from_type")
      .saveAsTable("index")
  }

}
