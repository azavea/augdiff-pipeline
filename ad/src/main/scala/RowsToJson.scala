package osmdiff

import org.apache.spark.sql.Row

import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

import scala.collection.mutable

import java.io._


object RowsToJson {

  private case class Properties(
    changeset: String,
    id: String,
    tags: Map[String, String],
    timestamp: String,
    tipe: String,
    uid: String,
    user: String,
    version: String,
    visible: Boolean
  )

  private case class LineStringFeature(
    id: String,
    geometry: LineString,
    properties: Properties,
    tipe: String = "Feature"
  )

  private case class PolygonFeature(
    id: String,
    geometry: Polygon,
    properties: Properties,
    tipe: String = "Feature"
  )

  private case class LineString(
    coordinates: Array[Array[Double]],
    tipe: String = "LineString"
  )

  private case class Polygon(
    coordinates: Array[Array[Array[Double]]],
    tipe: String = "Polygon"
  )

  private case class Point(
    coordinate: Array[Double],
    tipe: String = "Point"
  )

  def apply(filename: String, osm: Array[Row]) = {
    val points: Map[Long, Array[Double]] = osm
      .filter({ row => row.getString(2) /* type */ == "node" })
      .map({ row =>
        val id = row.getLong(1) /* id */
        try {
          val array = Array[Double](
            row.getDecimal(5).doubleValue, /* lon */
            row.getDecimal(4).doubleValue  /* lat */
          )
          (id, array)
        } catch {
          case e: Exception => (id, Array[Double](-1.0, -1.0))
        }
      })
      .toMap
    val notUnused = mutable.Set.empty[Long]
    val ways: Array[(Row, List[Long])] = osm
      .filter({ row => row.getString(2) /* type */ == "way" })
      .map({ row =>
        val list: List[Row] = row.getSeq(6).toList
        (row, list.map({ nd => nd.getLong(0) }))
      })

    val lines =
      ways.flatMap({ case (row, list) =>
        try {
          val id = row.getLong(1).toString
          val tipe = row.getString(2)
          val properties = Properties(
            changeset = row.getLong(8).toString,
            id = id,
            tags = row.getMap(3).asInstanceOf[Map[String, String]],
            timestamp = row.getTimestamp(9).toString,
            tipe = tipe,
            uid = row.getLong(10).toString,
            user = row.getString(11),
            version = row.getLong(12).toString,
            visible = row.getBoolean(13)
          )
          val coordinates: Array[Array[Double]] = list.flatMap({ id => points.get(id) }).toArray
          val json =
            if (list.head == list.last && list.length > 3) {
              val geometry = Polygon(coordinates = Array(coordinates))
              val feature = PolygonFeature(id = id, geometry = geometry, properties = properties)
              feature.asJson.noSpaces.toString.replaceAll("\"tipe\"", "\"type\"")
            }
            else {
              val geometry = LineString(coordinates = coordinates)
              val feature = LineStringFeature(id = id, geometry = geometry, properties = properties)
              feature.asJson.noSpaces.toString.replaceAll("\"tipe\"", "\"type\"")
            }
          Some(json)
        } catch {
          case e: Exception => None
        }
      })

    val fos = new FileOutputStream(new File(filename))
    val p = new java.io.PrintWriter(fos)
    var i = 0; while (i < lines.length) {
      p.write(lines(i)+"\n")
      p.flush
      i = i + 1
    }
    p.close

  }

}
