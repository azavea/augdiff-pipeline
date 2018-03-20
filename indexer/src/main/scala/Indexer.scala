package osmdiff.indexer

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import osmdiff.common.Common


object Indexer {

  def main(args: Array[String]): Unit = {
    val spark = Common.sparkSession("Indexer")

    val osm = spark.read.orc(args(0))
    val nodeToWays = osm
      .filter(col("type") === "way")
      .select(explode(col("nds.ref")).as("id"), col("timestamp").as("valid_from"), col("id").as("way_id"))
      .withColumn("valid_until", Common.farFuture())
      .withColumn("idstamp", Common.idTimestamp(col("id"), col("valid_from")))
    val xToRelations = osm
      .filter(col("type") === "relation")
      .select(explode(col("members")).as("id"), col("timestamp").as("valid_from"), col("id").as("relation_id"))
    val wayToRelations = xToRelations
      .filter(col("id.type") === "way")
      .select(col("id.ref").as("id"), col("valid_from"), col("relation_id"))
      .withColumn("valid_until", Common.farFuture())
      .withColumn("idstamp", Common.idTimestamp(col("id"), col("valid_from")))
    val relationToRelations  = xToRelations
      .filter(col("id.type") === "relation")
      .select(col("id.ref").as("id"), col("valid_from"), col("relation_id"))
      .withColumn("valid_until", Common.farFuture())
      .withColumn("idstamp", Common.idTimestamp(col("id"), col("valid_from")))

    osm
      .write
      .mode("overwrite")
      .format("orc")
      .sortBy("id").bucketBy(8, "id").partitionBy("type")
      .saveAsTable("osm")

    nodeToWays
      .write
      .mode("overwrite")
      .format("orc")
      .sortBy("idstamp").bucketBy(8, "idstamp")
      .saveAsTable("node_to_ways")

    wayToRelations
      .write
      .mode("overwrite")
      .format("orc")
      .sortBy("idstamp").bucketBy(8, "idstamp")
      .saveAsTable("way_to_relations")

    relationToRelations
      .write
      .mode("overwrite")
      .format("orc")
      .sortBy("idstamp").bucketBy(8, "idstamp")
      .saveAsTable("relation_to_relations")
  }

}
