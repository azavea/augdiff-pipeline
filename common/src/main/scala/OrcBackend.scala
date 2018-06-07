package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.orc
import org.apache.orc.storage.ql.exec.vector

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
    externalLocation: String,
    keyedTriples: Map[Long, Set[(Long, Long, String)]]
  ): DataFrame = {
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val partitions: Set[Long] = keyedTriples.keys.toSet
    val pairs: Set[(Long, String)] = keyedTriples.values.flatMap({ s => s.map({ t => (t._2, t._3) }) }).toSet
    val re = raw"p=(\d+)".r.unanchored
    val paths = mutable.ArrayBuffer.empty[Path]

    // https://stackoverflow.com/questions/11342400/how-to-list-all-files-in-a-directory-and-its-subdirectories-in-hadoop-hdfs
    val iter = fs.listFiles(new Path(externalLocation), true)
    while (iter.hasNext) {
      val path = iter.next.getPath
      path.toString match {
        case re(partition) => if (partitions.contains(partition.toLong)) paths.append(path)
        case _ =>
      }
    }

    // https://orc.apache.org/docs/core-java.html
    val options1 = orc.OrcFile.readerOptions(conf)
    paths.toArray.foreach({ path =>
      val reader = orc.OrcFile.createReader(path, options1)
      val options2 = reader.options
      val rows = reader.rows(options2)
      val batch = reader.getSchema.createRowBatch

      while(rows.nextBatch(batch)) {
        val ids = batch.cols(1).asInstanceOf[vector.LongColumnVector]
        val types = batch.cols(2).asInstanceOf[vector.BytesColumnVector]
      }

      rows.close
    })

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
