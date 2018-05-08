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
    mode: String
  ): Unit = {
    val options = externalLocation match {
      case Some(location) => Map("path"-> location)
      case None => Map.empty[String, String]
    }
    logger.info(s"Writing OSM as ORC files")
    bulk
      .orderBy("p", "id", "type")
      .write
      .mode(mode)
      .format("orc")
      .options(options)
      .partitionBy("p")
      .saveAsTable(tableName)
  }

}
