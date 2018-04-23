package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._


object PostgresBackend {

  private val logger = {
    val logger = Logger.getLogger(this.getClass)
    logger.setLevel(Level.INFO)
    logger
  }

  def saveBulk(
    bulk: DataFrame,
    uri: String, props: java.util.Properties,
    tableName: String, mode: String
  ): Unit = {
    logger.info(s"Writing OSM into ${uri}")
    bulk
      .write
      .mode(mode)
      .jdbc(uri, tableName, props)
  }

  def saveIndex(index: DataFrame, uri: String, props: java.util.Properties, tableName: String, mode: String): Unit = {
    logger.info(s"Writing index into ${uri}")
    index
      .write
      .mode(mode)
      .jdbc(uri, tableName, props)
  }

}
