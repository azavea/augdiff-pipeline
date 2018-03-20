package osmdiff.indexer

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
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

    val farFuture = {
      val c = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"))
      c.set(java.util.Calendar.YEAR, 1900+0xff)
      val ts = new java.sql.Timestamp(c.getTimeInMillis)
      udf({ () => ts })
    }

    val idTimestamp = udf({ (id: Long, ts: java.sql.Timestamp) =>
      val c = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"))
      c.setTime(ts)
      val year = c.get(java.util.Calendar.YEAR)-1900
      val month = c.get(java.util.Calendar.MONTH)
      val bits: Long = (id<<12) | ((0x000000ff & year)<<4) | (0x0000000f & month)
      bits // 12 bits for date, 48 bits for node id
    })

    val df: DataFrame = spark.read.orc(args(0))
    val nodeToWays: DataFrame = df
      .filter(col("type") === "way")
      .select(explode(col("nds.ref")).as("id"), col("timestamp").as("valid_from"), col("id").as("way_id"))
      .withColumn("valid_until", farFuture())
      .withColumn("idstamp", idTimestamp(col("id"), col("valid_from")))
    val xToRelations: DataFrame = df
      .filter(col("type") === "relation")
      .select(explode(col("members")).as("id"), col("timestamp").as("valid_from"), col("id").as("relation_id"))
    val wayToRelations: DataFrame = xToRelations
      .filter(col("id.type") === "way")
      .select(col("id.ref").as("id"), col("valid_from"), col("relation_id"))
      .withColumn("valid_until", farFuture())
      .withColumn("idstamp", idTimestamp(col("id"), col("valid_from")))
    val relationToRelations: DataFrame  = xToRelations
      .filter(col("id.type") === "relation")
      .select(col("id.ref").as("id"), col("valid_from"), col("relation_id"))
      .withColumn("valid_until", farFuture())
      .withColumn("idstamp", idTimestamp(col("id"), col("valid_from")))

    nodeToWays.printSchema
    wayToRelations.printSchema
    relationToRelations.printSchema

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
