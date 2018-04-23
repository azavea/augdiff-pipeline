package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._


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

}
