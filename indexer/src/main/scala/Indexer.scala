package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.rocksdb._


object Indexer {

  def main(args: Array[String]): Unit = {
    val spark = Common.sparkSession("Indexer")
    import spark.implicits._

    val osm = spark.read.orc(args(0))
    val nodeToWays = osm
      .filter(col("type") === "way")
      .select(explode(col("nds.ref")).as("id"), struct(col("timestamp").as("valid_from"), col("id").as("ptr")).as("ptrs"))
    val xToRelations = osm
      .filter(col("type") === "relation")
      .select(explode(col("members.ref")).as("id"), struct(col("timestamp").as("valid_from"), col("id").as("ptr")).as("ptrs"))
    val pointers = nodeToWays.union(xToRelations)
      .groupBy(col("id"))
      .agg(collect_list(col("ptrs")).as("ptrs"))
    val joined = osm.join(pointers, "id")

    joined.printSchema
    println(joined.head)

  }

}
