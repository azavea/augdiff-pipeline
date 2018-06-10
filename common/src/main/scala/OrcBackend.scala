package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import org.apache.orc
import org.apache.orc.storage.ql.exec.vector

import scala.collection.mutable


object OrcBackend {

  private val logger = {
    val logger = Logger.getLogger(this.getClass)
    logger.setLevel(Level.INFO)
    logger
  }

  def loadFile(
    path: Path,
    pairs: Set[(Long, String)],
    conf: Configuration
  ): Unit = {
    val reader = orc.OrcFile.createReader(path, orc.OrcFile.readerOptions(conf))
    val schema = reader.getSchema
    val rows = reader.rows(reader.options.schema(schema))
    val batch = schema.createRowBatch

    // https://orc.apache.org/docs/core-java.html
    while(rows.nextBatch(batch)) {
      val ids = batch.cols(0).asInstanceOf[vector.LongColumnVector] // id at 0 because p column dropped due to partitioned write
      val types = batch.cols(1).asInstanceOf[vector.BytesColumnVector]
      val tagss = batch.cols(2).asInstanceOf[vector.MapColumnVector]
      val tagKeys = tagss.keys.asInstanceOf[vector.BytesColumnVector]
      val tagValues = tagss.values.asInstanceOf[vector.BytesColumnVector]
      val lats = batch.cols(3).asInstanceOf[vector.DecimalColumnVector]
      val lons = batch.cols(4).asInstanceOf[vector.DecimalColumnVector]
      val ndss = batch.cols(5).asInstanceOf[vector.ListColumnVector]
      val ndssField = ndss.child.asInstanceOf[vector.StructColumnVector].fields(0).asInstanceOf[vector.LongColumnVector]
      val memberss = batch.cols(6).asInstanceOf[vector.ListColumnVector]
      val memberssChild = memberss.child.asInstanceOf[vector.StructColumnVector]
      val memberssTypes = memberssChild.fields(0).asInstanceOf[vector.BytesColumnVector]
      val memberssRefs = memberssChild.fields(1).asInstanceOf[vector.LongColumnVector]
      val memberssRoles = memberssChild.fields(2).asInstanceOf[vector.BytesColumnVector]

      Range(0, batch.size).foreach({ i =>

        // id
        val idIndex = if (ids.isRepeating) 0; else i
        val id: Long =
          if (ids.noNulls || !ids.isNull(idIndex)) ids.vector(idIndex)
          else -1

        // type
        val typeIndex = if (types.isRepeating) 0; else i
        val tipe: String =
          if (types.noNulls || !types.isNull(typeIndex)) {
            val start = types.start(typeIndex)
            val length = types.length(typeIndex)
            types.vector(typeIndex).drop(start).take(length).map(_.toChar).mkString
          }
          else null

        // tags
        val tagsIndex = if (tagss.isRepeating) 0; else i
        val tags: Map[String, String] =
          if (tagss.noNulls || !tagss.isNull(tagsIndex)) {
            val offset = tagss.offsets(tagsIndex).toInt
            val length = tagss.lengths(tagsIndex).toInt
            Range(offset, offset+length).map({ j =>
              val keyIndex = if (tagKeys.isRepeating) 0; else j
              val valueIndex = if (tagValues.isRepeating) 0; else j
              val key: String = tagKeys
                .vector(keyIndex)
                .drop(tagKeys.start(keyIndex))
                .take(tagKeys.length(keyIndex))
                .map(_.toChar).mkString
              val value: String = tagValues
                .vector(valueIndex)
                .drop(tagValues.start(valueIndex))
                .take(tagValues.length(valueIndex))
                .map(_.toChar).mkString
              key -> value
            }).toMap
          }
          else null

        // lat
        val latIndex = if (lats.isRepeating) 0; else i
        val lat: java.math.BigDecimal =
          if (lats.noNulls || !lats.isNull(latIndex))
            lats.vector(latIndex).getHiveDecimal.bigDecimalValue
          else null

        // lon
        val lonIndex = if (lons.isRepeating) 0; else i
        val lon: java.math.BigDecimal =
          if (lons.noNulls || !lons.isNull(lonIndex))
            lons.vector(lonIndex).getHiveDecimal.bigDecimalValue
          else null

        // nds
        val ndsIndex = if (ndss.isRepeating) 0; else i
        val nds: Array[Long] =
          if (ndss.noNulls || !ndss.isNull(ndsIndex)) {
            val offset = ndss.offsets(ndsIndex).toInt
            val length = ndss.lengths(ndsIndex).toInt
            Range(offset, offset+length).toArray.map({ j =>
              val index = if (ndssField.isRepeating) 0; else j
              ndssField.vector(index)
            })
          }
          else Array.empty[Long]

        // members
        val membersIndex = if (memberss.isRepeating) 0; else i
        val members: Array[Row] =
          if (memberss.noNulls || !memberss.isNull(membersIndex)) {
            val offset = memberss.offsets(membersIndex).toInt
            val length = memberss.lengths(membersIndex).toInt
            Range(offset, offset+length).toArray.map({ j =>
              val typeIndex = if (memberssTypes.isRepeating) 0; else j
              val refIndex = if (memberssRefs.isRepeating) 0; else j
              val roleIndex = if (memberssRoles.isRepeating) 0; else j
              val tipe2: String = memberssTypes
                .vector(typeIndex)
                .drop(memberssTypes.start(typeIndex))
                .take(memberssTypes.length(typeIndex))
                .map(_.toChar).mkString
              val ref: Long = memberssRefs.vector(refIndex)
              val role: String = memberssRoles
                .vector(roleIndex)
                .drop(memberssRoles.start(roleIndex))
                .take(memberssTypes.length(roleIndex))
                .map(_.toChar).mkString
              Row(tipe2, ref, role)
            })
          }
          else Array.empty[Row]

        val pair = (id, tipe)
        if (pairs.contains(pair)) {
          if (tipe == "node") {
            println(Row(id, tipe, tags, lat, lon, nds, members))
          }
          else if (tipe == "way") {
            println(Row(id, tipe, tags, null, null, nds, members))
            println(nds.toList)
          }
          else if (tipe == "relation") {
            println(Row(id, tipe, tags, null, null, nds, members))
            println(members.toList)
          }
        }
      })
    }
    rows.close
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

    paths.toArray.foreach({ path =>
      loadFile(path, pairs, conf)
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
