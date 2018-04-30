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

  // private def getRowHistories(
  //   rows: Array[Row],
  //   completePredicate: Row => Boolean,
  //   windowPredicate: Row => Boolean
  // ): Map[Long, RowHistory]

  def apply(filename: String, updateRows: Array[Row], allRows: Array[Row]) = {

    // The window of time covered by the rows
    val instants = updateRows.map({ row => row.getTimestamp(9).getTime })
    val startTime = instants.reduce(_ min _)
    val endTime = instants.reduce(_ max _)

    def nodeCompletePredicate(row: Row): Boolean = true

    def nodeWindowPredicate(row: Row): Boolean = { // XXX might need to be based on something other than time
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
          rowHistory.filter({ row => nodeWindowPredicate(row) }).take(1) match {
            case Stream(row) => Some(row)
            case Stream() => None
          }
        val beforeWindow: Option[Row] =
          rowHistory.filter({ row => !nodeWindowPredicate(row) }).take(1) match {
            case Stream(row) => Some(row)
            case Stream() => None
          }
        id -> RowHistory(inWindow = inWindow, beforeWindow = beforeWindow)
      }).toMap

    val nodeIds: Set[Long] = nodes.map(_._1).toSet

    def wayCompletePredicate(row: Row): Boolean = {
      val nds: List[Long] = row.getSeq(6).asInstanceOf[Seq[Row]].map(_.getLong(0)).toList
      nds.forall({ id => nodeIds.contains(id) })
    }

    def wayWindowPredicate(row: Row): Boolean = {
      if (nodeWindowPredicate(row)) true
      else {
        val nds: List[Long] = row.getSeq(6).asInstanceOf[Seq[Row]].map(_.getLong(0)).toList
        nds
          .map({ id => nodes.getOrElse(id, RowHistory(None, None)) })
          .exists({ row => row.inWindow != None })
      }
    }

    def wayNotWindowPredicate(row: Row): Boolean = {
      if (nodeWindowPredicate(row)) true
      else {
        val nds: List[Long] = row.getSeq(6).asInstanceOf[Seq[Row]].map(_.getLong(0)).toList
        nds
          .map({ id => nodes.getOrElse(id, RowHistory(None, None)) })
          .forall({ row => row.beforeWindow != None })
      }
    }

    // Relevant history of each way
    val ways: Map[Long, RowHistory] = allRows
      .filter({ row => row.getString(2) == "way" })
      .groupBy({ row => row.getLong(1) }).toArray
      .map({ case (id: Long, rows: Array[Row]) =>
        val rowHistory = rows.sortBy({ row => -row.getTimestamp(9).getTime }).toStream
        val inWindow: Option[Row] =
          rowHistory
            .filter({ row => wayCompletePredicate(row) && wayWindowPredicate(row) })
            .take(1) match {
            case Stream(row) => Some(row)
            case Stream() => None
          }
        val beforeWindow: Option[Row] =
          rowHistory
            .filter({ row => wayCompletePredicate(row) && wayNotWindowPredicate(row) })
            .take(1) match {
            case Stream(row) => Some(row)
            case Stream() => None
          }
        id -> RowHistory(inWindow = inWindow, beforeWindow = beforeWindow)
      }).toMap

    val wayIds: Set[Long] = ways.map(_._1).toSet

    val relationIds: Set[Long] = allRows
      .filter({ row => row.getString(2) == "relation" })
      .map({ row => row.getLong(1) })
      .toSet

    val _relations: Map[Long, Row] = allRows
      .filter({ row => row.getString(2) == "relation" })
      .groupBy({ row => row.getLong(1) })
      .map({ case (id: Long, rows: Array[Row]) =>
        id -> rows.sortBy({ row => -row.getTimestamp(9).getTime }).head
      }).toMap

    def relationCompletePredicate(row: Row): Boolean = {
      val members: List[Row] = row.getSeq(7).asInstanceOf[Seq[Row]].toList
      val nodeMembers = members.filter(_.getString(0) == "node").map(_.getLong(1))
      val wayMembers = members.filter(_.getString(0) == "way").map(_.getLong(1))
      val relMembers = members.filter(_.getString(0) == "relation").map(_.getLong(1))
      val nodesOkay = nodeMembers.forall({ id => nodeIds.contains(id) })
      val waysOkay = wayMembers.forall({ id => wayIds.contains(id) })
      val relsOkay = relMembers.forall({ id => relationIds.contains(id) })
      nodesOkay && waysOkay && relsOkay
    }

    def relationWindowPredicate(row: Row): Boolean = {
      if (nodeWindowPredicate(row)) true
      else {
        val members: List[Row] = row.getSeq(7).asInstanceOf[Seq[Row]].toList
        val nodeMembers = members.filter(_.getString(0) == "node").map(_.getLong(1))
        val wayMembers = members.filter(_.getString(0) == "way").map(_.getLong(1))
        val relMembers = members
          .filter(_.getString(0) == "relation")
          .map(_.getLong(1))
          .map({ id => _relations.getOrElse(id, throw new Exception("Oh no")) })
        val nodesOkay = nodeMembers
          .map({ id => nodes.getOrElse(id, RowHistory(None, None)) })
          .forall({ row => row.inWindow != None })
        val waysOkay = wayMembers
          .map({ id => ways.getOrElse(id, RowHistory(None, None)) })
          .forall({row => row.inWindow != None })
        val relsOkay = relMembers.exists({ row => relationWindowPredicate(row) })
        nodesOkay && waysOkay && relsOkay
      }
    }

    // Relevant history of each relation
    val relations: Map[Long, RowHistory] = allRows
      .filter({ row =>row.getString(2) == "relation" })
      .groupBy({ row => row.getLong(1) }).toArray
      .map({ case (id: Long, rows: Array[Row]) =>
        val rowHistory = rows.sortBy({ row => -row.getTimestamp(9).getTime }).toStream
        val inWindow: Option[Row] =
          rowHistory
            .filter({ row => relationCompletePredicate(row) && relationWindowPredicate(row) })
            .take(1) match {
            case Stream(row) => Some(row)
            case Stream() => None
          }
        val beforeWindow: Option[Row] =
          rowHistory
            .filter({ row => relationCompletePredicate(row) && !relationWindowPredicate(row) })
            .take(1) match {
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
