package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import cats.implicits._
import com.monovore.decline._


object Indexer extends CommandApp(
  name = "OSM Indexer",
  header = "Index OSM",
  main = {
    val spark = Common.sparkSession("Indexer")
    import spark.implicits._

    Common.denoise

    val orcfile =
      Opts.option[String]("orcfile", help = "ORC file containing OSM data")
    val partitions =
      Opts.option[Int]("partitions", help = "Number of partitions to use").orNone
    val persistence =
      Opts.option[String]("persistence", help = "Storage-level to use during indexing").orNone

    (orcfile, partitions, persistence).mapN({ (orcfile, partitions, _persistence) =>
      val persistence = _persistence match {
        case Some("memory_only") | Some("MEMORY_ONLY") => Some(StorageLevel.MEMORY_ONLY)
        case Some("memory_and_disk") | Some("MEMORY_AND_DISK") => Some(StorageLevel.MEMORY_AND_DISK)
        case Some("memory_only_ser") | Some("MEMORY_ONLY_SER") => Some(StorageLevel.MEMORY_ONLY_SER)
        case Some("memory_and_disk_ser") | Some("MEMORY_AND_DISK_SER") => Some(StorageLevel.MEMORY_AND_DISK_SER)
        case Some("disk_only") | Some("DISK_ONLY") => Some(StorageLevel.DISK_ONLY)
        case Some("off_heap") | Some("OFF_HEAP") => Some(StorageLevel.OFF_HEAP)
        case _ => None
      }

      val osm = spark
        .read.orc(orcfile)
        .withColumn("p", Common.partitionNumberUdf(col("id"), col("type")))
        .select(Common.osmColumns: _*)
      val index = ComputeIndex(osm, persistence, partitions)
      partitions match {
        case Some(p) => Common.saveIndex(index.repartition(p), "index", "overwrite")
        case None => Common.saveIndex(index, "index", "overwrite")
      }
      partitions match {
        case Some(p) => Common.saveBulk(osm.repartition(p), "osm", "overwrite")
        case None => Common.saveBulk(osm, "osm", "overwrite")
      }

    })
  }
)
