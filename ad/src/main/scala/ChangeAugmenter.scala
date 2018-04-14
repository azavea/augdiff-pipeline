package osmdiff

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import org.openstreetmap.osmosis.core.container.v0_6._
import org.openstreetmap.osmosis.core.domain.v0_6._
import org.openstreetmap.osmosis.core.task.v0_6.ChangeSink
import org.openstreetmap.osmosis.core.task.common.ChangeAction

import scala.collection.mutable

import java.sql.Timestamp


object ChangeAugmenter {

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

    Row(p, id, tipe, tags, lat, lon, nds, members, changeset, timestamp, uid, user, version, visible)
  }

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
    var lat: BigDecimal = null
    var lon: BigDecimal = null
    var nds: Array[Row] = Array.empty[Row]
    var members: Array[Row] = Array.empty[Row]
    var tipe: String = null

    entity.getType match {
      case EntityType.Node =>
        val node = entity.asInstanceOf[Node]
        tipe = "node"
        lat = BigDecimal(node.getLatitude)
        lon = BigDecimal(node.getLongitude)
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

    Row(p, id, tipe, tags, lat, lon, nds, members, changeset, timestamp, uid, user, version, visible)
  }

}

class ChangeAugmenter(spark: SparkSession) extends ChangeSink {
  import ChangeAugmenter._

  val ab = mutable.ArrayBuffer.empty[Row]

  def process(ct: ChangeContainer): Unit = {
    ct.getAction match {
      case ChangeAction.Create | ChangeAction.Modify =>
        ab.append(entityToRow(ct.getEntityContainer.getEntity, true))
      case ChangeAction.Delete =>
        ab.append(entityToLesserRow(ct.getEntityContainer.getEntity, false))
      case _ =>
    }

  }

  def initialize(m: java.util.Map[String,Object]): Unit = {
    println(s"initialize: ${m.entrySet.toArray.toList}")
  }

  def complete(): Unit = {
    println("complete")
  }

  def close(): Unit = {
    println("close")

    val window = Window.partitionBy("id", "type").orderBy(desc("timestamp"))
    val osm = spark.createDataFrame(
      spark.sparkContext.parallelize(ab.toList),
      StructType(Common.osmSchema))
    val lastLive = osm
      .withColumn("rank", rank().over(window))
      .filter(col("rank") === 1) // Most recent version of this id×type pair
      .select(col("id"), col("type"), col("timestamp"), col("visible"), col("nds"), col("members"))
    val index = Common.transitiveClosure(osm, Some(spark.table("index")))

    Common.saveBulk(osm.repartition(1), "osm_updates", "overwrite")
    Common.saveIndex(index.repartition(1), "index_updates", "overwrite")
  }

}
