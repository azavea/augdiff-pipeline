package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.orc.storage.ql.io.sarg._

import org.apache.orc
import org.apache.orc.storage.ql.exec.vector

import scala.collection.mutable


object OrcBackend {

  private val logger = {
    val logger = Logger.getLogger(this.getClass)
    logger.setLevel(Level.INFO)
    logger
  }

  def listFiles(
    conf: Configuration,
    paths: mutable.ArrayBuffer[Path],
    externalLocation: String
  ): Unit = {
    val fs = FileSystem.get(conf)

    logger.info(s"Getting list of ORC files")
    paths.clear

    // https://stackoverflow.com/questions/11342400/how-to-list-all-files-in-a-directory-and-its-subdirectories-in-hadoop-hdfs
    val iter = fs.listFiles(new Path(externalLocation), true)
    while (iter.hasNext) {
      val path = iter.next.getPath
      if (path.toString.endsWith(".orc")) paths.append(path)
    }
  }

  def loadFile(
    path: Path,
    sarg: SearchArgument,
    pairs: Set[(Long, String)],
    conf: Configuration
  ): Array[Row] = {
    val reader = orc.OrcFile.createReader(path, orc.OrcFile.readerOptions(conf))
    val schema = reader.getSchema
    val rows = reader.rows(reader.options.schema(schema).searchArgument(sarg, Array(null, "id")))
    val batch = schema.createRowBatch
    val ab = mutable.ArrayBuffer.empty[Row]

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
      val changesets = batch.cols(7).asInstanceOf[vector.LongColumnVector]
      val timestamps = batch.cols(8).asInstanceOf[vector.TimestampColumnVector]
      val uids = batch.cols(9).asInstanceOf[vector.LongColumnVector]
      val users = batch.cols(10).asInstanceOf[vector.BytesColumnVector]
      val versions = batch.cols(11).asInstanceOf[vector.LongColumnVector]
      val visibles = batch.cols(12).asInstanceOf[vector.LongColumnVector]

      Range(0, batch.size).foreach({ i =>

        // id
        val idIndex = if (ids.isRepeating) 0; else i
        val id: Long =
          if (ids.noNulls || !ids.isNull(idIndex)) ids.vector(idIndex)
          else Long.MinValue

        // type
        val typeIndex = if (types.isRepeating) 0; else i
        val tipe: String =
          if (types.noNulls || !types.isNull(typeIndex)) {
            val start = types.start(typeIndex)
            val length = types.length(typeIndex)
            types.vector(typeIndex).drop(start).take(length).map(_.toChar).mkString
          }
          else null

        // p, pair
        val p = Common.partitionNumberFn(id, tipe)
        val pair = (id, tipe)

        // tags
        val tagsIndex = if (tagss.isRepeating) 0; else i
        val tags: Map[String, String] =
          if ((tagss.noNulls || !tagss.isNull(tagsIndex)) && false) { // XXX
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
          else Map.empty[String, String]

        // lat
        val latIndex = if (lats.isRepeating) 0; else i
        val lat: java.math.BigDecimal =
          if ((lats.noNulls || !lats.isNull(latIndex)) && (tipe == "node"))
            lats.vector(latIndex).getHiveDecimal.bigDecimalValue
          else null

        // lon
        val lonIndex = if (lons.isRepeating) 0; else i
        val lon: java.math.BigDecimal =
          if ((lons.noNulls || !lons.isNull(lonIndex)) && (tipe == "node"))
            lons.vector(lonIndex).getHiveDecimal.bigDecimalValue
          else null

        // nds
        val ndsIndex = if (ndss.isRepeating) 0; else i
        val nds: Array[Row] =
          if ((ndss.noNulls || !ndss.isNull(ndsIndex)) && (tipe == "way")) {
            val offset = ndss.offsets(ndsIndex).toInt
            val length = ndss.lengths(ndsIndex).toInt
            Range(offset, offset+length).toArray.map({ j =>
              val index = if (ndssField.isRepeating) 0; else j
              Row(ndssField.vector(index))
            })
          }
          else Array.empty[Row]

        // members
        val membersIndex = if (memberss.isRepeating) 0; else i
        val members: Array[Row] =
          if ((memberss.noNulls || !memberss.isNull(membersIndex)) && (tipe == "relation")) {
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

        // changeset
        val changesetIndex = if (changesets.isRepeating) 0; else i
        val changeset =
          if (changesets.noNulls || !changesets.isNull(changesetIndex))
            changesets.vector(changesetIndex)
          else Long.MinValue

        // timestamp
        val timestampIndex = if (timestamps.isRepeating) 0; else i
        val timestamp =
          if (timestamps.noNulls || !timestamps.isNull(timestampIndex))
            new java.sql.Timestamp(timestamps.time(timestampIndex))
          else null

        // uid
        val uidIndex = if (uids.isRepeating) 0; else i
        val uid =
          if (uids.noNulls || !uids.isNull(uidIndex)) uids.vector(uidIndex)
          else Long.MinValue

        // user
        val userIndex = if (users.isRepeating) 0; else i
        val user: String =
          if (users.noNulls || !users.isNull(userIndex)) {
            val start = users.start(userIndex)
            val length = users.length(userIndex)
            users.vector(userIndex).drop(start).take(length).map(_.toChar).mkString
          }
          else null

        // version
        val versionIndex = if (versions.isRepeating) 0 ; else i
        val version =
          if (versions.noNulls || !versions.isNull(versionIndex)) versions.vector(versionIndex)
          else Long.MinValue

        // visible
        val visibleIndex = if (visibles.isRepeating) 0; else i
        val visible: Boolean =
          if (visibles.noNulls || !visibles.isNull(visibleIndex)) {
            if (visibles.vector(visibleIndex) == 0) false; else true
          }
          else false

        if (pairs.contains(pair)) {
          val row = Row(p, id, tipe, tags, lat, lon, nds, members, changeset, timestamp, uid, user, version, visible)
          ab.append(row)
        }
      })
    }

    rows.close
    ab.toArray
  }

  def load(
    conf: Configuration,
    paths: Array[Path],
    keyedTriples: Map[Long, Set[(Long, Long, String)]],
    pairs: Set[(Long, String)]
  ): Array[Row] = {
    val partitions: Set[Long] = keyedTriples.keys.toSet
    val re = raw"p=(\d+)".r.unanchored
    val paths2 = paths
      .filter({ path =>
        path.toString match {
          case re(partition) => if (partitions.contains(partition.toLong)) true; else false
          case _ => false
        }
      })

    logger.info(s"Building SearchArgument")
    val sarg: SearchArgument = {
      val longs = pairs.map({ pair => new java.lang.Long(pair._1) }).toArray
      SearchArgumentFactory
        .newBuilder
        .startOr
        .in("id", PredicateLeaf.Type.LONG, longs : _*)
        .end
        .build
    }

    logger.info(s"Loading ${keyedTriples.size} partitions from ${paths2.size} files")
    val rows = paths2.par.flatMap({ path => loadFile(path, sarg, pairs, conf) }).toArray

    logger.info(s"Got ${rows.size} rows from storage")

    rows
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
