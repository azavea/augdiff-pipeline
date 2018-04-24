package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.openstreetmap.osmosis.xml.common.CompressionMethod
import org.openstreetmap.osmosis.xml.v0_6.XmlChangeReader

import scala.collection.mutable

import java.io.File

import cats.implicits._
import com.monovore.decline._


object AugmentedDiff {

  private val logger = {
    val logger = Logger.getLogger(this.getClass)
    logger.setLevel(Level.INFO)
    logger
  }

  def augment(spark: SparkSession, rows: Array[Row]): Array[Row] = {
    val index = spark.table("index").select(Common.edgeColumns: _*) // index
      .union(spark.table("index_updates").select(Common.edgeColumns: _*))
    val osm = spark.table("osm").select(Common.osmColumns: _*) // osm
      .union(spark.table("osm_updates").select(Common.osmColumns: _*))
    val desired1 = rows.map({ r => (r.getLong(1) /* id */, r.getString(2) /* type */) }).toSet

    val pointers = OrcBackend.loadEdges(desired1, index)
      .map({ r => (r.getLong(0) /* ap */, r.getLong(1) /* aid */, r.getString(2) /* atype */) })

    val triples = pointers.groupBy(_._1)
    val desired2 = pointers.map({ p => (p._2, p._3) })
    logger.info(s"● Reading ${triples.size} partitions in groups of ${Common.pfLimit}")
    val dfs = triples.grouped(Common.pfLimit).map({ _group =>
      logger.info("● Reading group")
      val group = _group.toArray
      val ps = group.map({ kv => kv._1 })
      val ids = group.flatMap({ kv => kv._2.map(_._2) }).distinct
      val retval = osm.filter(col("p").isin(ps: _*))
      if (ids.length < Common.idLimit)
        retval.filter(col("id").isin(ids: _*))
      else
        retval
    })

    dfs.map({ df =>
      df.select(Common.osmColumns: _*)
        .collect
        .filter({ r => desired2.contains((r.getLong(1) /* id */, r.getString(2) /* type */)) })
    }).reduce(_ ++ _).distinct
  }

}

object AugmentedDiffApp extends CommandApp(
  name = "Augmented Differ",
  header = "Augment diffs",
  main = {
    val spark = Common.sparkSession("Augmented Diff")
    import spark.implicits._

    Common.denoise

    val oscfile =
      Opts.option[String]("oscfile", help = "OSC file containing OSM data").orNone
    val jsonfile =
      Opts.option[String]("jsonfile", help = "JSON file containing augmented diff").orNone
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

    (oscfile, jsonfile, postgresHost, postgresPort, postgresUser, postgresPassword, postgresDb).mapN({
      (oscfile, jsonfile, postgresHost, postgresPort, postgresUser, postgresPassword, postgresDb) =>

      val uri = s"jdbc:postgresql://${postgresHost}:${postgresPort}/${postgresDb}"
      val props = {
        val ps = new java.util.Properties()
        ps.put("user", postgresUser)
        ps.put("password", postgresPassword)
        ps.put("driver", "org.postgresql.Driver")
        ps
      }

      oscfile match {
        case Some(oscfile) =>
          val cr = new XmlChangeReader(new File(oscfile), true, CompressionMethod.None)
          val ca = new ChangeAugmenter(spark, uri, props)
          cr.setChangeSink(ca)
          cr.run
        case None =>
      }

      jsonfile match {
        case Some(jsonfile) =>
          val updates = spark.table("osm_updates").select(Common.osmColumns: _*).collect
          println(s"updates: ${updates.length}")
          val time1 = System.currentTimeMillis
          println(s"size: ${AugmentedDiff.augment(spark, updates).length}")
          val time2 = System.currentTimeMillis
          val augmented = AugmentedDiff.augment(spark, updates)
          println(s"size: ${augmented.length}")
          val time3 = System.currentTimeMillis
          println(s"times: ${time2 - time1} ${time3 - time2}")
          RowsToJson(jsonfile, augmented)
        case None =>
      }
    })
  }
)
