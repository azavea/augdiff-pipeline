package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


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
      .filter(col("a_to_b") === true)
      .select(
        Common.pairToLongUdf(col("aid"), col("atype")).as("a"),
        Common.pairToLongUdf(col("bid"), col("btype")).as("b"))
      .groupBy(col("a")).agg(collect_set(col("b")).as("bs"))
      .write
      .mode(mode)
      .jdbc(uri, tableName, props)
  }

}
