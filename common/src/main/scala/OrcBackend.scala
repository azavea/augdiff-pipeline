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

  def saveBulk(
    bulk: DataFrame,
    tableName: String,
    externalLocation: Option[String],
    partitions: Option[Int],
    mode: String
  ): Unit = {
    val options = externalLocation match {
      case Some(location) => Map("path"-> location)
      case None => Map.empty[String, String]
    }
    logger.info(s"Writing OSM as ORC files")
    val sorted = partitions match {
      case Some(n) => bulk.orderBy("p", "id", "type").repartition(n)
      case None => bulk.orderBy("p", "id", "type")
    }
    sorted
      .write
      .mode(mode)
      .format("orc")
      .options(options)
      .partitionBy("p")
      .saveAsTable(tableName)
  }

}
