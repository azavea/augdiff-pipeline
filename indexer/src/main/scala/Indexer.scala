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

    val osm = spark.read.orc(args(0))
    val index = Common.transitiveClosure(osm, None)

    if (args.length > 1) {
      val n = args(1).toInt
      Common.saveBulk(osm.repartition(n), "osm", "overwrite")
      Common.saveIndex(index.repartition(n), "index", "overwrite")
    } else {
      Common.saveBulk(osm, "osm", "overwrite")
      Common.saveIndex(index, "index", "overwrite")
    }

  }

}
