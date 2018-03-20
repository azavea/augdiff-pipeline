package osmdiff.ad

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql.functions._

import osmdiff.common.Common

import scala.collection.mutable


object AugmentedDiff {

  val spark = Common.sparkSession("Augmented Diff")
  import spark.implicits._

  val nodeToWays = spark.table("node_to_ways")
  val wayToRelations = spark.table("way_to_relations")
  val relationToRelations = spark.table("relation_to_relations")

  private def recurseNode(nodeId: Long) = {
    var keepGoing = true
    val relations = mutable.Set.empty[Long]

    val ways = nodeToWays
      .filter(col("id") === nodeId)
      .map({ r => r.getAs[Long]("way_id") })
      .collect
      .toSet

    relations ++=
    (if (ways.isEmpty)
      Set.empty[Long]
    else {
      wayToRelations
        .filter(col("id") >= ways.min)
        .filter(col("id") <= ways.max)
        .filter({ r => ways.contains(r.getAs[Long]("id")) })
        .map({ r => r.getAs[Long]("relation_id") })
        .collect
        .toSet
    })

    while (keepGoing) {
      keepGoing = false
      val newRelations =
        if (relations.isEmpty) Set.empty[Long]
        else {
          relationToRelations
            .filter(col("id") >= relations.min)
            .filter(col("id") <= relations.max)
            .filter({ r => relations.contains(r.getAs[Long]("id")) })
            .map({ r => r.getAs[Long]("relation_id") })
            .collect
            .toSet
        }
      if (!newRelations.subsetOf(relations)) {
        keepGoing = true
        relations ++= newRelations
      }
    }

    (ways, relations)
  }

  def main(args: Array[String]): Unit = {
    val nodeId = args(0).toLong
    val (ways, relations) = recurseNode(nodeId)

    println(s"node=$nodeId ways=$ways relations=$relations")
  }

}
