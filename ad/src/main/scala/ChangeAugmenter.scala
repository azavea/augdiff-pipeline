package osmdiff

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import org.openstreetmap.osmosis.core.container.v0_6._
import org.openstreetmap.osmosis.core.domain.v0_6._
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType
import org.openstreetmap.osmosis.core.task.v0_6.ChangeSink
import org.openstreetmap.osmosis.core.task.common.ChangeAction

import scala.collection.mutable

import java.sql.Timestamp


object ChangeAugmenter {

  val ndsSchema = ArrayType(StructType(List(StructField("ref", LongType, true))))
  val membersSchema = ArrayType(StructType(List(
    StructField("type", StringType, true),
    StructField("ref", LongType, true),
    StructField("role", StringType, true))))
  val osmSchema = StructType(List(
    StructField("id", LongType, true),
    StructField("tags", MapType(StringType, StringType), true),
    StructField("lat", DecimalType(9, 7), true),
    StructField("lon", DecimalType(10, 7), true),
    StructField("nds", ndsSchema, true),
    StructField("members", membersSchema, true),
    StructField("changeset", LongType, true),
    StructField("timestamp", TimestampType, true),
    StructField("uid", LongType, true),
    StructField("user", StringType, true),
    StructField("version", LongType, true),
    StructField("visible", BooleanType, true),
    StructField("type", StringType, true)))

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
    val nds = null
    val members = null
    val typeString: String = entity.getType match {
      case EntityType.Node => "node"
      case EntityType.Way => "way"
      case EntityType.Relation => "relation"
      case _ => throw new Exception
    }

    Row(id, tags, lat, lon, nds, members, changeset, timestamp, uid, user, version, visible, typeString)
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
    var nds: Array[Row] = null
    var members: Array[Row] = null
    var typeString: String = null

    entity.getType match {
      case EntityType.Node =>
        val node = entity.asInstanceOf[Node]
        typeString = "node"
        lat = BigDecimal(node.getLatitude)
        lon = BigDecimal(node.getLongitude)
      case EntityType.Way =>
        val way = entity.asInstanceOf[Way]
        typeString = "way"
        nds = way.getWayNodes.toArray.map({ wayNode => Row(wayNode.asInstanceOf[WayNode].getNodeId) })
      case EntityType.Relation =>
        val relation = entity.asInstanceOf[Relation]
        typeString = "relation"
        members = relation.getMembers.toArray.map({ relationMember =>
          val rm = relationMember.asInstanceOf[RelationMember]
          val typeString2 = rm.getMemberType match {
            case EntityType.Node => "node"
            case EntityType.Way => "way"
            case EntityType.Relation => "relation"
            case _ => throw new Exception
          }
          val ref = rm.getMemberId
          val role = rm.getMemberRole
          Row(typeString2, ref, role)
        })
      case _ => throw new Exception
    }

    Row(id, tags, lat, lon, nds, members, changeset, timestamp, uid, user, version, visible, typeString)
  }

}

class ChangeAugmenter(spark: SparkSession) extends ChangeSink {
  import ChangeAugmenter._

  val ab = mutable.ArrayBuffer.empty[Row]

  def process(ct: ChangeContainer): Unit = {
    val et = ct.getEntityContainer

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

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(ab.toList),
      StructType(osmSchema)
    )

    println(s"${df.count}")
    spark.table("osm")
      .union(df)
      .printSchema
    println(s"${df.head}")
  }

  def close(): Unit = {
    println("close")
  }

}
