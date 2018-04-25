package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable


object OrcBackend {

  private val logger = {
    val logger = Logger.getLogger(this.getClass)
    logger.setLevel(Level.INFO)
    logger
  }

  def saveBulk(bulk: DataFrame, tableName: String, mode: String): Unit = {
    logger.info(s"Writing OSM as ORC files")
    bulk
      .orderBy("p", "id", "type")
      .write
      .mode(mode)
      .format("orc")
      .partitionBy("p")
      .saveAsTable(tableName)
  }

  def saveIndex(index: DataFrame, tableName: String, mode: String): Unit = {
    logger.info(s"Writing index as ORC files")
    index
      .orderBy("bp", "bid", "btype")
      .write
      .mode(mode)
      .format("orc")
      .partitionBy("bp")
    .saveAsTable(tableName)
  }

  def loadEdges(desired: Set[(Long, String)], edges: DataFrame): mutable.Set[Row] = {
    val pairs = desired.groupBy({ pair => Common.partitionNumberFn(pair._1, pair._2) })
    logger.info(s"◼ Reading ${pairs.size} partitions in groups of ${Common.pfLimit}") // 175 bug (pfLimit <= 175)
    val dfs = pairs.grouped(Common.pfLimit).map({ _group =>
      logger.info("◼ Reading group")
      val group = _group.toArray
      val ps = group.map({ kv => kv._1 })
      val ids = group.flatMap({ kv => kv._2.map(_._1) }).distinct
      val retval = edges.filter(col("bp").isin(ps: _*)) // partition pruning
      if (ids.length < Common.idLimit)
        retval.filter(col("bid").isin(ids: _*)) // predicate pushdown
      else retval
    })
    val s = mutable.Set.empty[Row]
    dfs.foreach({ df =>
      s ++= df.select(Common.edgeColumns: _*)
        .collect
        .filter({ r => desired.contains((r.getLong(5) /* bid */, r.getString(6) /* btype */)) })
    })
    s
  }

}
