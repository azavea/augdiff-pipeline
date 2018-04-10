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

  val window1 = Window.partitionBy("prior_id", "prior_type").orderBy(desc("instant"))
  val window2 = Window.partitionBy("id", "type").orderBy(desc("timestamp"))

  def augment(rows: DataFrame) = {
    val dependents = spark.table("index").union(spark.table("index_updates"))
      .join(
        rows,
        ((col("prior_id") === col("id")) &&
         (col("prior_type") === col("type"))),
        "left_semi")
      .withColumn("row_number", row_number().over(window1))
      .filter(col("row_number") === 1)
      .select(col("dependent_id").as("id"), col("dependent_type").as("type"), col("instant"))
      .distinct
    val priors = spark.table("index").union(spark.table("index_updates")).as("left")
      .join(
        dependents.as("right"),
        ((col("left.dependent_id") === col("right.id")) &&
         (col("left.dependent_type") === col("right.type"))),
        "left_semi")
      .withColumn("row_number", row_number().over(window1))
      .filter(col("row_number") === 1)
      .select(col("prior_id").as("id"), col("prior_type").as("type"), col("instant"))
      .distinct
    spark.table("osm").union(spark.table("osm_updates")).as("left")
      .join(
        dependents.union(priors).as("right"),
        ((col("left.id") === col("right.id")) &&
         (col("left.type") === col("right.type")) &&
         Common.getInstant(col("left.timestamp")) >= (col("right.instant"))),
        "left_semi")
      .withColumn("row_number", row_number().over(window2))
      .filter(col("row_number") === 1)
      .drop("row_number")
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
      println(s"size: ${augment(updates).count}")
      val time3 = System.currentTimeMillis
      println(s"times: ${time2 - time1} ${time3 - time2}")
    }
  }

}
