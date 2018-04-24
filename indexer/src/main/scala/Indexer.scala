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
    val postgresHost =
      Opts.option[String]("postgresHost", help = "PostgreSQL host").withDefault("localhost")
    val postgresPort =
      Opts.option[Int]("postgresPort", help = "PostgreSQL port").withDefault(5432)
    val postgresUser =
      Opts.option[String]("postgresUser", help = "PostgreSQL username").withDefault("hive")
    val postgresPassword =
      Opts.option[String]("postgresPassword", help = "PostgreSQL password").withDefault("hive")
    val postgresDb =
      Opts.option[String]("postgresDb", help = "PostgreSQL database").withDefault("osm")

    (orcfile, partitions, persistence, postgresHost, postgresPort, postgresUser, postgresPassword, postgresDb).mapN({
      (orcfile, partitions, _persistence, postgresHost, postgresPort, postgresUser, postgresPassword, postgresDb) =>

      val persistence = _persistence match {
        case Some("memory_only") | Some("MEMORY_ONLY") => Some(StorageLevel.MEMORY_ONLY)
        case Some("memory_and_disk") | Some("MEMORY_AND_DISK") => Some(StorageLevel.MEMORY_AND_DISK)
        case Some("memory_only_ser") | Some("MEMORY_ONLY_SER") => Some(StorageLevel.MEMORY_ONLY_SER)
        case Some("memory_and_disk_ser") | Some("MEMORY_AND_DISK_SER") => Some(StorageLevel.MEMORY_AND_DISK_SER)
        case Some("disk_only") | Some("DISK_ONLY") => Some(StorageLevel.DISK_ONLY)
        case Some("off_heap") | Some("OFF_HEAP") => Some(StorageLevel.OFF_HEAP)
        case _ => None
      }
      val uri = s"jdbc:postgresql://${postgresHost}:${postgresPort}/${postgresDb}"
      val props = {
        val ps = new java.util.Properties()
        ps.put("user", postgresUser)
        ps.put("password", postgresPassword)
        ps.put("driver", "org.postgresql.Driver")
        ps
      }

      val osm = spark
        .read.orc(orcfile)
        .withColumn("p", Common.partitionNumberUdf(col("id"), col("type")))
        .select(Common.osmColumns: _*)
      val index = ComputeIndex(osm, persistence, partitions)
      PostgresBackend.saveIndex(index, uri, props, "index", "overwrite")

      // partitions match {
      //   case Some(p) => OrcBackend.saveBulk(osm.repartition(p), "osm", "overwrite")
      //   case None => OrcBackend.saveBulk(osm, "osm", "overwrite")
      // }

    })
  }
)
