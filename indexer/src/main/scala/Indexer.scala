package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel


object Indexer {

  def main(args: Array[String]): Unit = {
    val spark = Common.sparkSession("Indexer")
    import spark.implicits._

    Common.denoise

    val osm = spark
      .read.orc(args(0))
      .withColumn("p", Common.partitionNumberUdf(col("id"), col("type")))
      .select(Common.osmColumns: _*)
    val persistence = if (args.length > 1) {
      args(1) match {
        case "memory_only" | "MEMORY_ONLY" => Some(StorageLevel.MEMORY_ONLY)
        case "memory_and_disk" | "MEMORY_AND_DISK" => Some(StorageLevel.MEMORY_AND_DISK)
        case "memory_only_ser" | "MEMORY_ONLY_SER" => Some(StorageLevel.MEMORY_ONLY_SER)
        case "memory_and_disk_ser" | "MEMORY_AND_DISK_SER" => Some(StorageLevel.MEMORY_AND_DISK_SER)
        case "disk_only" | "DISK_ONLY"=> Some(StorageLevel.DISK_ONLY)
        case "off_heap" | "OFF_HEAP" => Some(StorageLevel.OFF_HEAP)
        case _ => None
      }
    }
    else None

    val partitions =
      if (args.length > 2) Some(args(2).toInt)
      else None
    val index = ComputeIndex(osm, persistence, partitions)

    if (args.length > 2) {
      val p = args(2).toInt
      Common.saveIndex(index.repartition(p), "index", "overwrite")
      Common.saveBulk(osm.repartition(p), "osm", "overwrite")
    } else {
      Common.saveIndex(index, "index", "overwrite")
      Common.saveBulk(osm, "osm", "overwrite")
    }

  }

}
