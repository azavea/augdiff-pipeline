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

  val window1 = Window.partitionBy("left.a", "left.b").orderBy(desc("instant"))
  val window2 = Window.partitionBy("left.id", "left.type").orderBy(desc("timestamp"))

  def augment(rows: DataFrame) = { // XXX only works in the present
    val index = spark.table("index").select(Common.edgeColumns: _*) // search index
      .union(spark.table("index_updates").select(Common.edgeColumns: _*))
    val osm = spark.table("osm").select(Common.osmColumns: _*) // OSM bulk
      .union(spark.table("osm_updates").select(Common.osmColumns: _*))
    val pointers = index.as("left") // Use the index to get entities
                                    // touched by the rows, and all
                                    // entities that those entities
                                    // depend on
      .join(
        rows.as("right"),
        ((col("left.bp") === col("right.p")) && // Partition pruning
         (col("left.b.id") === col("right.id")) && // Same id
         (col("left.b.type") === col("right.type"))), // Same type
        "left_semi")
      .withColumn("rank", rank().over(window1))
      .filter(col("rank") === 1)
      .select(
        col("left.a.id").as("id"),
        col("left.a.type").as("type"),
        col("left.ap").as("p"))
      .union(rows.select(
        col("id").as("id"),
        col("type").as("type"),
        col("p").as("p")))

    osm.as("left")
      .join(
      pointers.as("right"),
        ((col("left.p") === col("right.p")) && // Partition pruning
         (col("left.id") === col("right.id")) && // Same id
         (col("left.type") === col("right.type"))), // Same type
        "left_semi")
      .withColumn("rank", rank().over(window2))
      .filter(col("rank") === 1)
      .select(Common.osmColumns: _*)
  }

  def main(args: Array[String]): Unit = {

    Common.denoise

    if (args(0) != "xxx") {
      val cr = new XmlChangeReader(new File(args(0)), true, CompressionMethod.None)
      val ca = new ChangeAugmenter(spark)
      cr.setChangeSink(ca)
      cr.run
    }
    else {
      val updates = spark.table("osm_updates")
      println(s"updates: ${updates.count}")
      val time1 = System.currentTimeMillis
      println(s"size: ${augment(updates).count}")
      val time2 = System.currentTimeMillis
      val augmented = augment(updates)
      println(s"size: ${augmented.count}")
      val time3 = System.currentTimeMillis
      println(s"times: ${time2 - time1} ${time3 - time2}")

      if ((args.length > 1) && (args(1) == "yyy")) {
        updates.select(Common.osmColumns: _*).foreach({ row => println(s"◯ $row") })
        println
        augmented.select(Common.osmColumns: _*).foreach({ row => println(s"◯ $row") })
      }
    }
  }

}
