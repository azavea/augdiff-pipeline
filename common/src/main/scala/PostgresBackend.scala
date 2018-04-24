package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable


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

  def saveIndex(
    index: DataFrame,
    uri: String, props: java.util.Properties,
    tableName: String, mode: String
  ): Unit = {
    logger.info(s"Writing index into ${uri}")
    index
      .select(
        Common.pairToLongUdf(col("aid"), col("atype")).as("a"),
        col("a_to_b").as("direction"),
        Common.pairToLongUdf(col("bid"), col("btype")).as("b"))
      .write
      .mode(mode)
      .jdbc(uri, tableName, props)
  }

  def saveIndex(
    index: Set[ComputeIndexLocal.Edge],
    uri: String, props: java.util.Properties,
    tableName: String, mode: String
  ): Unit = {
    logger.info(s"Writing index into ${uri}")
    val connection = java.sql.DriverManager.getConnection(uri, props)
    ???
  }

  def loadEdges(
    desired: Set[Long],
    uri: String, props: java.util.Properties
  ): mutable.Set[ComputeIndexLocal.Edge] = {
    val connection = java.sql.DriverManager.getConnection(uri, props)
    ???
  }

}
