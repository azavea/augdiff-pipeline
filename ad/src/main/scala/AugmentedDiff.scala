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

  val logger = {
    val logger = Logger.getLogger(this.getClass)
    logger.setLevel(Level.INFO)
    logger
  }

  def augment(
    spark: SparkSession,
    rows: Array[Row],
    uri: String, props: java.util.Properties
  ): Array[Row] = {
    val osm = spark.table("osm").select(Common.osmColumns: _*)
    val rowLongs = rows.map({ row =>
      val id = row.getLong(1)
      val tipe = row.getString(2)
      Common.pairToLongFn(id, tipe)
    }).toSet
    val triples1 = // from updates
      rows.map({ row =>
        val id = row.getLong(1)
        val tipe = row.getString(2)
        val p = Common.partitionNumberFn(id, tipe)
        (p, id, tipe)
      }).toSet
    val triples2 = // frp, dependencies
      PostgresBackend.loadEdges(rowLongs, uri, props)
        .map({ edge =>
          val long = if (edge.direction == true) edge.a ; else edge.b
          val id = Common.longToIdFn(long)
          val tipe = Common.longToTypeFn(long)
          val p = Common.partitionNumberFn(id, tipe)
          (p, id, tipe)
        }).toSet
    val triples = triples1 ++ triples2
    val desired = triples.map({ triple => (triple._2, triple._3) })
    val keyedTriples = triples.groupBy(_._1)

    logger.info(s"● Reading ${keyedTriples.size} partitions in groups of ${Common.pfLimit}")
    val dfs: Iterator[DataFrame] = keyedTriples.grouped(Common.pfLimit).map({ triples =>
      logger.info("● Reading group")
      val ps: Array[Long] = triples.map(_._1).toArray
      val ids: Array[Long] = triples.map(_._2).reduce(_ ++ _).map(_._2).toArray
      val retval: DataFrame = osm.filter(col("p").isin(ps: _*))
      if (ids.length < Common.idLimit)
        retval.filter(col("id").isin(ids: _*))
      else
        retval
    })

    dfs
      .map({ df =>
        df.select(Common.osmColumns: _*)
          .collect
          .filter({ row => desired.contains((row.getLong(1) /* id */, row.getString(2) /* type */)) })
      })
      .reduce(_ ++ _).distinct
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
          val updates = spark.table("inbox").select(Common.osmColumns: _*).collect
          val time1 = System.currentTimeMillis
          RowsToJson(jsonfile, updates, AugmentedDiff.augment(spark, updates, uri, props))
          val time2 = System.currentTimeMillis
          AugmentedDiff.logger.info(s"Augmented diff produced in ${time2 - time1} ms")
        case None =>
      }
    })
  }
)
