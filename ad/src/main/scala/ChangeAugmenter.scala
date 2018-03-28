package osmdiff

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import org.openstreetmap.osmosis.core.container.v0_6._
import org.openstreetmap.osmosis.core.task.v0_6.ChangeSink


object ChangeAugmenter {

  val ndsSchema = ArrayType(StructType(List(StructField("ref", LongType, true))))
  val membersSchema = ArrayType(StructType(List(
    StructField("type", StringType, true),
    StructField("ref", LongType, true),
    StructField("role", StringType, true))))
  val osmSchema = List(
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
    StructField("type", StringType, true)
  )
}

class ChangeAugmenter(spark: SparkSession) extends ChangeSink {
  var counter = 0L

  def process(ct: ChangeContainer): Unit = {
    val action = ct.getAction
    val et = ct.getEntityContainer

    counter=counter+1
    if (et.getEntity.getId == 6197499L) {
      et match {
        case nc: NodeContainer => println(s"${nc.getEntity}")
        case wc: WayContainer => println(s"${wc.getEntity}")
        case rc: RelationContainer => println(s"${rc.getEntity}")
        case _ => throw new Exception
      }

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
  }
}
