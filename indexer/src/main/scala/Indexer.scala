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

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val osm = spark.read.orc(args(0))
    val index = Common.transitiveClosure(osm, None)

    osm
      .write
      .mode("overwrite")
      .format("orc")
      .sortBy("id", "type", "timestamp").bucketBy(1, "id", "type")
      .saveAsTable("osm")
    index
      .write
      .mode("overwrite")
      .format("orc")
      .sortBy("from_id", "from_type", "instant").bucketBy(1, "from_id", "from_type")
      .saveAsTable("index")
  }

}
