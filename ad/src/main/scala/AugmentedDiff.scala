package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import org.openstreetmap.osmosis.xml.common.CompressionMethod
import org.openstreetmap.osmosis.xml.v0_6.XmlChangeReader

import scala.collection.mutable

import java.io.File


object AugmentedDiff {

  val spark = Common.sparkSession("Augmented Diff")
  import spark.implicits._

  val window1 = Window.partitionBy("way_id").orderBy(desc("instant"))
  val window2 = Window.partitionBy("relation_id").orderBy(desc("instant"))

  def recurseNodes(nodes: DataFrame, ways: DataFrame, relations: DataFrame) = {

    var keepGoing = true

    val touchedWays = spark.table("node_to_ways").union(spark.table("node_to_ways_updates"))
      .join(nodes, col("id") === col("other_id"), "inner")
      .withColumn("row_number", row_number().over(window1))
      .filter(col("row_number") === 1) // XXX only works for "now"
      .select(col("way_id"), col("instant"))
      .orderBy(col("way_id"))

    var touchedRelations = {
      val relationsFromNodes = spark.table("node_to_relations").union(spark.table("node_to_relations_updates"))
        .join(nodes, col("id") === col("other_id"), "inner")
        .withColumn("row_number", row_number().over(window2))
        .filter(col("row_number") === 1) // XXX
        .select(col("relation_id"), col("instant"))
      val relationsFromWays = spark.table("way_to_relations").union(spark.table("way_to_relations_updates"))
        .join(ways, col("id") === col("other_id"), "inner")
        .withColumn("row_number", row_number().over(window2))
        .filter(col("row_number") === 1) // XXX
        .select(col("relation_id"), col("instant"))
      val relationFromWays2 = spark.table("way_to_relations").union(spark.table("way_to_relations_updates"))
        .join(
          touchedWays.select(col("way_id"), col("instant").as("way_instant")),
          col("id") === col("way_id"),
          "inner")
        .filter(col("instant") <= col("way_instant")) // relation must be older than way
        .drop("way_id", "way_instant", "other_id")
        .withColumn("row_number", row_number().over(window2))
        .filter(col("row_number") === 1) // XXX
        .select(col("relation_id"), col("instant"))
      val relationsFromRelations = spark.table("relation_to_relations").union(spark.table("relation_to_relations"))
        .join(relations, col("id") === col("other_id"), "inner")
        .withColumn("row_number", row_number().over(window2))
        .filter(col("row_number") === 1) // XXX
        .select(col("relation_id"), col("instant"))
      relationsFromNodes
        .union(relationsFromWays)
        .union(relationFromWays2)
        .union(relationsFromRelations)
        .orderBy(col("relation_id"))
        .distinct
    }
    var previousRelations: Long = 0

    while (touchedRelations.count > previousRelations) {
      println(s"${previousRelations} ${touchedRelations.count}")
      val relationsFromRelations = spark.table("relation_to_relations")
        .union(spark.table("relation_to_relations_updates"))
        .join(
          touchedRelations.select(col("relation_id").as("other_id"), col("instant").as("other_instant")),
          col("id") === col("other_id"),
          "inner")
        .filter(col("instant") <= col("other_instant")) // dependant relation must be older than independant relation
        .drop("other_id", "other_instant")
        .withColumn("row_number", row_number().over(window2))
        .filter(col("row_number") === 1) // XXX
        .select(col("relation_id"), col("instant"))
      previousRelations = touchedRelations.count
      touchedRelations = touchedRelations.union(relationsFromRelations).orderBy(col("relation_id")).distinct
    }

    (touchedWays.collect, touchedRelations.collect)
  }

  // private def recurseNode(nodeId: Long) = {
  //   var keepGoing = true
  //   val relations = mutable.Set.empty[Long]

  //   val ways = nodeToWays
  //     .filter(col("id") === nodeId)
  //     .map({ r => r.getAs[Long]("way_id") })
  //     .collect
  //     .toSet

  //   relations ++=
  //   (if (ways.isEmpty)
  //     Set.empty[Long]
  //   else {
  //     wayToRelations
  //       .filter(col("id").isin(ways.toSeq:_*))
  //       .map({ r => r.getAs[Long]("relation_id") })
  //       .collect
  //       .toSet
  //   })

  //   while (keepGoing) {
  //     keepGoing = false
  //     val newRelations =
  //       if (relations.isEmpty) Set.empty[Long]
  //       else {
  //         relationToRelations
  //           .filter(col("id").isin(relations.toSeq:_*))
  //           .map({ r => r.getAs[Long]("relation_id") })
  //           .collect
  //           .toSet
  //       }
  //     if (!newRelations.subsetOf(relations)) {
  //       keepGoing = true
  //       relations ++= newRelations
  //     }
  //   }

  //   (ways, relations)
  // }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    if (args(0) != "xxx") {
      val cr = new XmlChangeReader(new File(args(0)), true, CompressionMethod.None)
      val ca = new ChangeAugmenter(spark)
      cr.setChangeSink(ca)
      cr.run
    }
    else {
      val updates = spark.table("osm_updates")
        .sample(0.05)
        // .sample(0.00001)
        .orderBy(col("id"))
        .select(col("id").as("other_id"), col("type"))
        .distinct
      val nodes = updates.filter(col("type") === "node").drop(col("type"))
      val ways = updates.filter(col("type") === "way").drop(col("type"))
      val relations = updates.filter(col("type") === "relation").drop(col("type"))

      val time1 = System.currentTimeMillis
      val (ways1, relations1) = recurseNodes(nodes, ways, relations)
      val time2 = System.currentTimeMillis
      val (ways2, relations2) = recurseNodes(nodes, ways, relations)
      val time3 = System.currentTimeMillis
      println(s"${time2 - time1} ${time3 - time2}")
    }
  }

}
