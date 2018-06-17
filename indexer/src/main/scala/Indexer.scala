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
    val external =
      Opts.option[String]("external", help = "External location of OSM table")

    (orcfile, postgresHost, postgresPort, postgresUser, postgresPassword, postgresDb, external).mapN({
      (orcfile, postgresHost, postgresPort, postgresUser, postgresPassword, postgresDb, _external) =>

      val external =
        if (_external.endsWith("/")) _external
        else _external + "/"

      val uri = s"jdbc:postgresql://${postgresHost}:${postgresPort}/${postgresDb}"
      val props = {
        val ps = new java.util.Properties()
        ps.put("user", postgresUser)
        ps.put("password", postgresPassword)
        ps.put("driver", "org.postgresql.Driver")
        ps
      }

      val osm = spark.read.orc(orcfile)
      OrcBackend.saveOsm(osm, "osm", external + "osm/", "overwrite")

      val combIndex = ComputeIndex(osm)
      OrcBackend.saveIndex(combIndex, "index", external + "index/", "overwrite")
      PostgresBackend.saveIndex(combIndex, uri, props, "index", "overwrite")
    })
  }
)
