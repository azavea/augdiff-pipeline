package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable


object OrcBackend {

  private val logger = {
    val logger = Logger.getLogger(this.getClass)
    logger.setLevel(Level.INFO)
    logger
  }

  def load(
    spark: SparkSession,
    tableName: String,
    externalLocation: String
  ): DataFrame = {
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // https://stackoverflow.com/questions/11342400/how-to-list-all-files-in-a-directory-and-its-subdirectories-in-hadoop-hdfs
    val iter = fs.listFiles(new Path(externalLocation), true)
    while (iter.hasNext) {
      val path = iter.next.getPath
      println(path)
    }

    spark.table("osm").select(Common.osmColumns: _*)
  }

  def save(
    df: DataFrame,
    tableName: String,
    externalLocation: String,
    mode: String
  ): Unit = {
    val options = Map(
      "orc.bloom.filter.columns" -> "id",
      "orc.create.index" -> "true",
      "orc.row.index.stride" -> "1000",
      "path" -> externalLocation
    )

    logger.info(s"Writing OSM as ORC files")
    df
      .repartition(col("p"))
      .sortWithinPartitions(col("id"), col("type"))
      .write
      .mode(mode)
      .format("orc")
      .options(options)
      .partitionBy("p")
      .saveAsTable(tableName)
  }

}
