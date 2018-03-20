package osmdiff.ad

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql.functions._

import osmdiff.common.Common


object AugmentedDiff {

  def main(args: Array[String]): Unit = {
    val spark = Common.sparkSession("Augmented Diff")
  }

}
