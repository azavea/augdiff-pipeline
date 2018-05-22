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
    val options1 = Map(
      "orc.create.index" -> "true",
      "orc.row.index.stride" -> "1000",
      "orc.bloom.filter.columns" -> "id"
    )
    val options2 = externalLocation match {
      case Some(location) => Map("path"-> location)
      case None => Map.empty[String, String]
    }

    logger.info(s"Writing OSM as ORC files")
    bulk
      .repartition(col("p"))
      .sortWithinPartitions(col("id"), col("type"))
      .write
      .mode(mode)
      .format("orc")
      .options(options1 ++ options2)
      .partitionBy("p")
      .saveAsTable(tableName)
  }

}
