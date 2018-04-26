package osmdiff

import org.apache.spark.sql.Row

import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

import geotrellis.vector._
import geotrellis.vector.io._

import spray.json._
import spray.json.DefaultJsonProtocol._

import scala.collection.mutable

import java.io._


object RowsToJson {

  sealed case class AlteredRow(row: Row, changeset: Long, until: Long, visible: Boolean)

  def apply(filename: String, _rows: Array[Row]) = {

    val alteredRows = _rows
      .groupBy({ row => (row.getLong(1) /* id */, row.getString(2) /* type */) })
      .map({ case ((id: Long, tipe: String), rows: Array[Row]) =>
        val pair = rows.sortBy({ row => -row.getTimestamp(9).getTime }).take(2) match {
          case Array(current, previous) =>
            if (current.getBoolean(13) /* visible */ == true) { // visible
              val current2 = AlteredRow(
                row = current,
                changeset = current.getLong(8),
                until = -1,
                visible = true)
              val previous2 = AlteredRow(
                row = previous,
                changeset = previous.getLong(8),
                until = current.getLong(8),
                visible = previous.getBoolean(13))
              Array(current2, previous2)
            }
            else { // invisible
              val current2 = AlteredRow(
                row = previous,
                changeset = current.getLong(8),
                until = -1,
                visible = false)
              val previous2 = AlteredRow(
                row = previous,
                changeset = previous.getLong(8),
                until = current.getLong(8),
                visible = previous.getBoolean(13))
              Array(current2, previous2)
            }
          case Array(current) =>
            Array(AlteredRow(
              row = current,
              changeset = current.getLong(8),
              until = -1,
              visible = current.getBoolean(13)))

          case _ => Array.empty[AlteredRow]
        }

        (id, tipe) -> pair
      })

    // val points: Map[Long, Array[Double]] = osm
    //   .filter({ row => row.getString(2) /* type */ == "node" })
    //   .map({ row =>
    //     val id = row.getLong(1) /* id */
    //     try {
    //       val array = Array[Double](
    //         row.getDecimal(5).doubleValue, /* lon */
    //         row.getDecimal(4).doubleValue  /* lat */
    //       )
    //       (id, array)
    //     } catch {
    //       case e: Exception => (id, Array[Double](-1.0, -1.0))
    //     }
    //   })
    //   .toMap
    // val notUnused = mutable.Set.empty[Long]
    // val ways: Array[(Row, List[Long])] = osm
    //   .filter({ row => row.getString(2) /* type */ == "way" })
    //   .map({ row =>
    //     val list: List[Row] = row.getSeq(6).toList
    //     (row, list.map({ nd => nd.getLong(0) }))
    //   })

    // val lines =
    //   ways.flatMap({ case (row, list) =>
    //     try {
    //       val id = row.getLong(1).toString
    //       val tipe = row.getString(2)
    //       val properties = Properties(
    //         changeset = row.getLong(8).toString,
    //         id = id,
    //         tags = row.getMap(3).asInstanceOf[Map[String, String]],
    //         timestamp = row.getTimestamp(9).toString,
    //         tipe = tipe,
    //         uid = row.getLong(10).toString,
    //         user = row.getString(11),
    //         version = row.getLong(12).toString,
    //         visible = row.getBoolean(13)
    //       )
    //       val coordinates: Array[Array[Double]] = list.flatMap({ id => points.get(id) }).toArray
    //       val json =
    //         if (list.head == list.last && list.length > 3) {
    //           val geometry = Polygon(coordinates = Array(coordinates))
    //           val feature = PolygonFeature(id = id, geometry = geometry, properties = properties)
    //           feature.asJson.noSpaces.toString.replaceAll("\"tipe\"", "\"type\"")
    //         }
    //         else {
    //           val geometry = LineString(coordinates = coordinates)
    //           val feature = LineStringFeature(id = id, geometry = geometry, properties = properties)
    //           feature.asJson.noSpaces.toString.replaceAll("\"tipe\"", "\"type\"")
    //         }
    //       Some(json)
    //     } catch {
    //       case e: Exception => None
    //     }
    //   })

    // val fos = new FileOutputStream(new File(filename))
    // val p = new java.io.PrintWriter(fos)
    // var i = 0; while (i < lines.length) {
    //   p.write(lines(i)+"\n")
    //   p.flush
    //   i = i + 1
    // }
    // p.close

  }

}
