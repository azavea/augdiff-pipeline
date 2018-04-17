package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import org.openstreetmap.osmosis.xml.common.CompressionMethod
import org.openstreetmap.osmosis.xml.v0_6.XmlChangeReader

import scala.collection.mutable

import java.io.File


object AugmentedDiff {

  val spark = Common.sparkSession("Augmented Diff")
  import spark.implicits._


  private val logger = {
    val logger = Logger.getLogger(this.getClass)
    logger.setLevel(Level.INFO)
    logger
  }

  private def augment(rows: Array[Row]): Array[Row] = { // XXX too many timestamps
    val index = spark.table("index").select(Common.edgeColumns: _*) // index
      .union(spark.table("index_updates").select(Common.edgeColumns: _*))
    val osm = spark.table("osm").select(Common.osmColumns: _*) // osm
      .union(spark.table("osm_updates").select(Common.osmColumns: _*))
    val desired1 = rows.map({ r => (r.getLong(1) /* id */, r.getString(2) /* type */) }).toSet

    val pointers = Common.loadEdges(desired1, index)
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

  def main(args: Array[String]): Unit = {

    Common.denoise

    if (args(0) != "xxx") {
      val cr = new XmlChangeReader(new File(args(0)), true, CompressionMethod.None)
      val ca = new ChangeAugmenter(spark)
      cr.setChangeSink(ca)
      cr.run
    }
    else {
      val updates = spark.table("osm_updates").select(Common.osmColumns: _*).collect
      println(s"updates: ${updates.length}")
      val time1 = System.currentTimeMillis
      println(s"size: ${augment(updates).length}")
      val time2 = System.currentTimeMillis
      val augmented = augment(updates)
      println(s"size: ${augmented.length}")
      val time3 = System.currentTimeMillis
      println(s"times: ${time2 - time1} ${time3 - time2}")

      if ((args.length > 1) && (args(1) == "yyy")) {
        updates.foreach({ row => println(s"◯ $row") })
        println
        augmented.foreach({ row => println(s"◯◯ $row") })
      }
    }
  }

}
