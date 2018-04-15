package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object Indexer {

  def main(args: Array[String]): Unit = {
    val spark = Common.sparkSession("Indexer")
    import spark.implicits._

    Common.denoise

    val osm = spark
      .read.orc(args(0))
      .withColumn("p", Common.partitionNumberUdf(col("id"), col("type")))
      .select(Common.osmColumns: _*)
    val index = ComputeIndex(osm)

    Common.saveBulk(osm, "osm", "overwrite")
    Common.saveIndex(index, "index", "overwrite")
  }

}
