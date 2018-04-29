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
  sealed case class RowHistory(inWindow: Option[Row], beforeWindow: Option[Row])

  def apply(filename: String, updateRows: Array[Row], allRows: Array[Row]) = {

    // The window of time covered by the rows
    val instants = updateRows.map({ row => row.getTimestamp(9).getTime })
    val startTime = instants.reduce(_ min _)
    val endTime = instants.reduce(_ max _)

    // Relevant history of each node
    val nodes: Array[(Long, RowHistory)] = _rows
      .filter({ row => row.getString(2) == "node" })
      .groupBy({ row => row.getLong(1) }).toArray
      .map({ case (id: Long, rows: Array[Row]) =>
        val rowHistory: RowHistory =
          rows.sortBy({ row => -row.getTimestamp(9).getTime }).take(2) match {
            case Array(newer, older) =>
              val newerTime = newer.getTimestamp(9).getTime
              val olderTime = older.getTimestamp(9).getTime
              val newerIn = (startTime <= newerTime && newerTime <= endTime)
              val olderIn = (startTime <= olderTime && olderTime <= endTime)
              val bits = (newerIn, olderIn)
              bits match {
                case (true, true) => RowHistory(inWindow = Some(newer), beforeWindow = None)
                case (true, false) => RowHistory(inWindow = Some(newer), beforeWindow = Some(older))
                case (false, false) => RowHistory(inWindow = None, beforeWindow = Some(newer))
                case (false, true) => throw new Exception("Oh no")
              }
            case Array(newer) =>
              val newerTime = newer.getTimestamp(9).getTime
              val newerIn = (startTime <= newerTime && newerTime <= endTime)
              newerIn match {
                case true => RowHistory(inWindow = Some(newer), beforeWindow = None)
                case false => RowHistory(inWindow = None, beforeWindow = Some(newer))
              }
            case _ => throw new Exception("Oh no")
          }
        (id, rowHistory)
      })

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
