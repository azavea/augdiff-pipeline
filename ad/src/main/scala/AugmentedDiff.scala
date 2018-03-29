package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql.functions._

import org.openstreetmap.osmosis.xml.common.CompressionMethod
import org.openstreetmap.osmosis.xml.v0_6.XmlChangeReader

import scala.collection.mutable

import java.io.File


object AugmentedDiff {

  val spark = Common.sparkSession("Augmented Diff")
  import spark.implicits._

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

    val cr = new XmlChangeReader(new File(args(0)), true, CompressionMethod.None)
    val ca = new ChangeAugmenter(spark)
    cr.setChangeSink(ca)
    cr.run

  }

}
