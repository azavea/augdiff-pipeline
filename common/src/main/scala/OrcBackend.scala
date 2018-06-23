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

  // ************************************************************************

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

  // ************************************************************************

  def loadOsmFile(
    path: Path,
    sarg: SearchArgument,
    idtypes: Set[Long],
    conf: Configuration
  ): Array[Row] = {
    val reader = orc.OrcFile.createReader(path, orc.OrcFile.readerOptions(conf))
    val schema = reader.getSchema
    val rows = reader.rows(reader.options.schema(schema).searchArgument(sarg, Array(null, "id")))
    val batch = schema.createRowBatch
    val ab = mutable.ArrayBuffer.empty[Row]

    // https://orc.apache.org/docs/core-java.html
    while(rows.nextBatch(batch)) {
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
      val idtypesCol = batch.cols(13).asInstanceOf[vector.LongColumnVector]

      Range(0, batch.size).foreach({ i =>

        // idtype, id, type, p
        val idtypeIndex = if (idtypesCol.isRepeating) 0; else i
        val idtype: Long =
          if (idtypesCol.noNulls || !idtypesCol.isNull(idtypeIndex)) idtypesCol.vector(idtypeIndex)
          else Long.MinValue
        val id = Common.longToIdFn(idtype)
        val tipe = Common.longToTypeFn(idtype)
        val p = Common.partitionNumberFn(idtype)

        if (idtypes.contains(idtype)) {

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

          val row = Row(p, id, tipe, tags, lat, lon, nds, members, changeset, timestamp, uid, user, version, visible, idtype)
          ab.append(row)
        }
      })
    }

    rows.close
    ab.toArray
  }

  def loadOsm(
    conf: Configuration,
    paths: Array[Path],
    idtypes: Set[Long]
  ): Array[Row] = {
    val partitions: Set[Long] = idtypes.map({ idtype => Common.partitionNumberFn(idtype) })
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
      val longs = idtypes.map(new java.lang.Long(_)).toArray
      SearchArgumentFactory
        .newBuilder
        .startOr
        .in("idtype", PredicateLeaf.Type.LONG, longs : _*)
        .end
        .build
    }

    logger.info(s"Loading ${idtypes.size} potential rows in ${partitions.size} partitions from ${paths2.size} files")
    val rows = paths2.par.flatMap({ path => loadOsmFile(path, sarg, idtypes, conf) }).toArray

    logger.info(s"Got ${rows.size} rows from storage")

    rows
  }

  def saveOsm(
    df: DataFrame,
    tableName: String,
    externalLocation: String,
    mode: String
  ): Unit = {
    val options = Map(
      "orc.bloom.filter.columns" -> "idtype",
      "orc.create.index" -> "true",
      "orc.row.index.stride" -> "1024",
      "path" -> externalLocation
    )

    logger.info(s"Writing OSM as ORC files")
    df
      .withColumn("p", Common.partitionNumberUdf(col("id"), col("type")))
      .withColumn("idtype", Common.idTypeToLongUdf(col("id"), col("type")))
      .select(Common.osmColumns: _*)
      .repartition(col("p"))
      .sortWithinPartitions(col("idtype"))
      .write
      .mode(mode)
      .format("orc")
      .options(options)
      .partitionBy("p")
      .saveAsTable(tableName)
  }

  // ************************************************************************

  def loadIndexFile(
    path: Path,
    sarg: SearchArgument,
    sourceSet: Set[Long],
    conf: Configuration,
    aIndex: Boolean
  ): Array[Row] = {
    val reader = orc.OrcFile.createReader(path, orc.OrcFile.readerOptions(conf))
    val schema = reader.getSchema
    val sourceColumn: String = if (aIndex) "a"; else "b"
    val sourceColIndex: Int = if (aIndex) 0; else 1
    val destColIndex: Int = if (aIndex) 1; else 0
    val rows = reader.rows(reader.options.schema(schema).searchArgument(sarg, Array(null, sourceColumn)))
    val batch = schema.createRowBatch
    val ab = mutable.ArrayBuffer.empty[Row]

    while(rows.nextBatch(batch)) {
      val sources = batch.cols(sourceColIndex).asInstanceOf[vector.LongColumnVector] // a column
      val dests = batch.cols(destColIndex).asInstanceOf[vector.LongColumnVector] // b column

      Range(0, batch.size).foreach({ i =>
        val sourceIndex = if (sources.isRepeating) 0; else i
        val source: Long =
          if (sources.noNulls || !sources.isNull(sourceIndex)) sources.vector(sourceIndex)
          else Long.MinValue

        if (sourceSet.contains(source)) {
          val destIndex = if (dests.isRepeating) 0; else i
          val dest: Long =
            if (dests.noNulls || !dests.isNull(destIndex)) dests.vector(destIndex)
            else Long.MinValue

          val row = Row(source, dest)
          ab.append(row)
        }
      })
    }

    rows.close
    ab.toArray
  }

  def loadIndex(
    conf: Configuration,
    paths: Array[Path],
    idtypes: Set[Long],
    aIndex: Boolean
  ): Array[Row] = {
    val partitions: Set[Long] = idtypes.map({ idtype => Common.partitionNumberFn(idtype) })
    val re = raw"p=(\d+)".r.unanchored
    val paths2 = paths
      .filter({ path =>
        path.toString match {
          case re(partition) => if (partitions.contains(partition.toLong)) true; else false
          case _ => false
        }
      })

    logger.info(s"Building SearchArgument")
    val sarg: SearchArgument = if (aIndex) {
      val longs = idtypes.map(new java.lang.Long(_)).toArray
      SearchArgumentFactory
        .newBuilder
        .startOr
        .in("a", PredicateLeaf.Type.LONG, longs : _*)
        .end
        .build
    }
    else {
      val longs = idtypes.map(new java.lang.Long(_)).toArray
      SearchArgumentFactory
        .newBuilder
        .startOr
        .in("a", PredicateLeaf.Type.LONG, longs : _*)
        .end
        .build
    }

    logger.info(s"Loading ${idtypes.size} potential rows in ${partitions.size} partitions from ${paths2.size} files")
    val rows = paths2.par.flatMap({ path => loadIndexFile(path, sarg, idtypes, conf, aIndex) }).toArray

    logger.info(s"Got ${rows.size} rows from storage")

    rows
  }

  def saveIndex(
    df: DataFrame,
    tableName: String,
    externalLocation: String,
    mode: String
  ): Unit = {
    val optionsA = Map(
      "orc.bloom.filter.columns" -> "a",
      "orc.create.index" -> "true",
      "orc.row.index.stride" -> "1024",
      "path" -> (externalLocation + "a/")
    )
    val optionsB = Map(
      "orc.bloom.filter.columns" -> "b",
      "orc.create.index" -> "true",
      "orc.row.index.stride" -> "1024",
      "path" -> (externalLocation + "b/")
    )

    logger.info(s"Writing index A as ORC files")
    df
      .withColumn("p", Common.partitionNumberUdf2(col("a")))
      .select(Common.indexColumns: _*)
      .repartition(col("p"))
      .sortWithinPartitions(col("a"))
      .write
      .mode(mode)
      .format("orc")
      .options(optionsA)
      .partitionBy("p")
      .saveAsTable("a" + tableName)

    logger.info(s"Writing index B as ORC files")
    df
      .withColumn("p", Common.partitionNumberUdf2(col("b")))
      .select(Common.indexColumns: _*)
      .repartition(col("p"))
      .sortWithinPartitions(col("b"))
      .write
      .mode(mode)
      .format("orc")
      .options(optionsB)
      .partitionBy("p")
      .saveAsTable("b" + tableName)
  }

}
