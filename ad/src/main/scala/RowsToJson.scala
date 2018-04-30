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

    def windowPredicate(row: Row): Boolean = {
      val instant = row.getTimestamp(9).getTime
      (startTime <= instant && instant <= endTime)
    }

    // Relevant history of each node
    val nodes: Map[Long, RowHistory] = allRows
      .filter({ row => row.getString(2) == "node" })
      .groupBy({ row => row.getLong(1) }).toArray
      .map({ case (id: Long, rows: Array[Row]) =>
        val rowHistory = rows.sortBy({ row => -row.getTimestamp(9).getTime }).toStream
        val inWindow: Option[Row] =
          rowHistory.filter({ row => windowPredicate(row) }).take(1) match {
            case Stream(row) => Some(row)
            case Stream() => None
          }
        val beforeWindow: Option[Row] =
          rowHistory.filter({ row => !windowPredicate(row) }).take(1) match {
            case Stream(row) => Some(row)
            case Stream() => None
          }

        id -> RowHistory(inWindow = inWindow, beforeWindow = beforeWindow)
      }).toMap

    def wayWindowPredicate(row: Row): Boolean = {
      val nds: List[Long] = row.getSeq(6).asInstanceOf[Seq[Row]].map(_.getLong(0)).toList
      val rowInWindow = windowPredicate(row)
      lazy val depsInWindow =  nds
        .map({ id => nodes.getOrElse(id, RowHistory(None, None)) })
        .forall({ rh => rh.inWindow != None })
      rowInWindow || depsInWindow
    }

    // Relevant history of each way
    val ways: Map[Long, RowHistory] = allRows
      .filter({ row => row.getString(2) == "way" })
      .groupBy({ row => row.getLong(1) }).toArray
      .map({ case (id: Long, rows: Array[Row]) =>
        val rowHistory = rows.sortBy({ row => -row.getTimestamp(9).getTime }).toStream
        val inWindow: Option[Row] =
          rowHistory.filter({ row => wayWindowPredicate(row) }).take(1) match {
            case Stream(row) => Some(row)
            case Stream() => None
          }
        val beforeWindow: Option[Row] =
          rowHistory.filter({ row => !wayWindowPredicate(row) }).take(1) match {
            case Stream(row) => Some(row)
            case Stream() => None
          }

        id -> RowHistory(inWindow = inWindow, beforeWindow = beforeWindow)
      }).toMap

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
