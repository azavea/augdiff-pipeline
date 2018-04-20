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
    val index =
      if (args.length > 1) ComputeIndex(osm, Some(args(1).toInt))
      else ComputeIndex(osm, None)

    if (args.length > 1) {
      val p = args(1).toInt
      Common.saveIndex(index.repartition(p), "index", "overwrite")
      Common.saveBulk(osm.repartition(p), "osm", "overwrite")
    } else {
      Common.saveIndex(index, "index", "overwrite")
      Common.saveBulk(osm, "osm", "overwrite")
    }

  }

}
