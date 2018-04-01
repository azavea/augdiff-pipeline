package osmdiff

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.SparkSession


object Common {

  def sparkSession(appName: String): SparkSession = {
    val conf = new SparkConf()
      .setAppName(appName)
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.executor.heartbeatInterval", "30")
      .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .set("spark.sql.hive.metastorePartitionPruning", "true")
      .set("spark.sql.orc.filterPushdown", "true")

    SparkSession.builder
      .config(conf)
      .enableHiveSupport
      .getOrCreate
  }

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

}
