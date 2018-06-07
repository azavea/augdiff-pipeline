package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import org.apache.hadoop.fs.{FileSystem, Path}

import org.openstreetmap.osmosis.core.container.v0_6._
import org.openstreetmap.osmosis.core.domain.v0_6._
import org.openstreetmap.osmosis.core.task.v0_6.ChangeSink
import org.openstreetmap.osmosis.core.task.common.ChangeAction

import scala.collection.mutable

import java.io._
import java.net.URI
import java.sql.Timestamp


object ChangeAugmenter {

  // Lesser rows have all of the same columns, but the information for
  // many of those columns is not provided (used for recording
  // deletions).
  def entityToLesserRow(entity: Entity, visible: Boolean): Row = {
    val id: Long = entity.getId
    val tags = Map.empty[String,String]
    val changeset = null
    val timestamp: Timestamp = new Timestamp(entity.getTimestamp.getTime)
    val uid = null
    val user = null
    val version: Long = entity.getVersion
    val lat = null
    val lon = null
    val nds = Array.empty[Row]
    val members = Array.empty[Row]
    val tipe: String = entity.getType match {
      case EntityType.Node => "node"
      case EntityType.Way => "way"
      case EntityType.Relation => "relation"
      case _ => throw new Exception
    }

    val p = Common.partitionNumberFn(id, tipe)
    val row = Row(p, id, tipe, tags, lat, lon, nds, members, changeset, timestamp, uid, user, version, visible)

    row
  }

  // These rows come from creations and modifications.
  def entityToRow(entity: Entity, visible: Boolean): Row = {
    val id: Long = entity.getId
    val tags: Map[String,String] = entity.getTags.toArray.map({ tag =>
      val t = tag.asInstanceOf[Tag]
      (t.getKey -> t.getValue)
    }).toMap
    val changeset: Long = entity.getChangesetId
    val timestamp: Timestamp = new Timestamp(entity.getTimestamp.getTime)
    val uid: Long = entity.getUser.getId
    val user: String = entity.getUser.getName
    val version: Long = entity.getVersion
    var lat: java.math.BigDecimal = null
    var lon: java.math.BigDecimal = null
    var nds: Array[Row] = Array.empty[Row]
    var members: Array[Row] = Array.empty[Row]
    var tipe: String = null

    entity.getType match {
      case EntityType.Node =>
        val node = entity.asInstanceOf[Node]
        tipe = "node"
        lat = new java.math.BigDecimal(node.getLatitude)
        lon = new java.math.BigDecimal(node.getLongitude)
      case EntityType.Way =>
        val way = entity.asInstanceOf[Way]
        tipe = "way"
        nds = way.getWayNodes.toArray.map({ wayNode => Row(wayNode.asInstanceOf[WayNode].getNodeId) })
      case EntityType.Relation =>
        val relation = entity.asInstanceOf[Relation]
        tipe = "relation"
        members = relation.getMembers.toArray.map({ relationMember =>
          val rm = relationMember.asInstanceOf[RelationMember]
          val tipe2 = rm.getMemberType match {
            case EntityType.Node => "node"
            case EntityType.Way => "way"
            case EntityType.Relation => "relation"
            case _ => throw new Exception
          }
          val ref = rm.getMemberId
          val role = rm.getMemberRole
          Row(tipe2, ref, role)
        })
      case _ => throw new Exception
    }

    val p = Common.partitionNumberFn(id, tipe)
    val row = Row(p, id, tipe, tags, lat, lon, nds, members, changeset, timestamp, uid, user, version, visible)

    row
  }

}

class ChangeAugmenter(
  spark: SparkSession,
  uri: String, props: java.util.Properties,
  jsonfile: String,
  externalLocation: String
) extends ChangeSink {
  import ChangeAugmenter._

  val osm = mutable.ArrayBuffer.empty[Row]

  val logger = {
    val logger = Logger.getLogger(this.getClass)
    logger.setLevel(Level.INFO)
    logger
  }

  def process(ct: ChangeContainer): Unit = {
    ct.getAction match {
      case ChangeAction.Create | ChangeAction.Modify =>
        val r = entityToRow(ct.getEntityContainer.getEntity, true)
        osm.append(r)
      case ChangeAction.Delete =>
        val r = entityToLesserRow(ct.getEntityContainer.getEntity, false)
        osm.append(r)
      case _ =>
    }

  }

  def initialize(m: java.util.Map[String,Object]): Unit = {}

  def complete(): Unit = {
    logger.info("complete")

    val diff = osm.toArray
    val osmDf = spark.createDataFrame(
      spark.sparkContext.parallelize(diff, 1),
      StructType(Common.osmSchema))
    val (newEdges, allEdges) = ComputeIndexLocal(diff, uri, props)
    val augmentedDiff = AugmentedDiff.augment(spark, diff, allEdges)
    val fos =
      if (jsonfile.startsWith("hdfs:") || jsonfile.startsWith("s3a:") || jsonfile.startsWith("file:")) {
        val path = new Path(jsonfile)
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem.get(new URI(jsonfile), conf)
        fs.create(path)
      }
      else new FileOutputStream(new File(jsonfile))

    RowsToJson(fos, diff, augmentedDiff)
    PostgresBackend.saveIndex(newEdges, uri, props, "index")
    OrcBackend.saveBulk(osmDf, "osm", externalLocation, "append")
  }

  def close(): Unit = {}

}
