package osmdiff.indexer

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession


object Indexer {

  def sparkSession(): SparkSession = {
    val conf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setAppName(s"Indexer")
      .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .set("spark.sql.hive.metastorePartitionPruning", "true")
      .set("spark.sql.orc.filterPushdown", "true")

    SparkSession.builder
      .config(conf)
      .enableHiveSupport
      .getOrCreate
  }

  def main(args: Array[String]): Unit = {
    val spark = sparkSession
    val df = spark.read.orc(args(0)).cache
    val nodeToWay: DataFrame = df
      .filter(col("type") === "way")
      .select(explode(col("nds.ref")).as("id"), col("timestamp"), col("id").as("way_id"))

    nodeToWay.printSchema
  }

}
