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

  sealed case class RowHistory(inWindow: Option[Row], beforeWindow: Option[Row])

  private def getRowHistories(
    rows: Array[Row],
    tipe: String,
    completePredicate: Row => Boolean,
    windowPredicate: Row => Boolean,
    beforePredicate: Row => Boolean
  ): Map[Long, RowHistory] = {
    rows
      .filter({ row =>row.getString(2) == tipe })
      .groupBy({ row => row.getLong(1) }).toArray
      .map({ case (id: Long, rows: Array[Row]) =>
        val rowHistory = rows.sortBy({ row => -row.getTimestamp(9).getTime }).toStream
        val inWindow: Option[Row] =
          rowHistory
            .filter({ row => completePredicate(row) && windowPredicate(row) })
            .take(1) match {
            case Stream(row) => Some(row)
            case Stream() => None
          }
        val beforeWindow: Option[Row] =
          rowHistory
            .filter({ row => completePredicate(row) && beforePredicate(row) })
            .take(1) match {
            case Stream(row) => Some(row)
            case Stream() => None
          }
        id -> RowHistory(inWindow = inWindow, beforeWindow = beforeWindow)
      }).toMap
  }

  def apply(filename: String, updateRows: Array[Row], allRows: Array[Row]) = {

    // The window of time covered by the rows
    val instants = updateRows.map({ row => row.getTimestamp(9).getTime })
    val startTime = instants.reduce(_ min _)
    val endTime = instants.reduce(_ max _)

    /*********** NODES ***********/

    def nodeCompletePredicate(row: Row): Boolean = true

    def nodeWindowPredicate(row: Row): Boolean = { // XXX might need to be based on something other than time
      val instant = row.getTimestamp(9).getTime
      (startTime <= instant && instant <= endTime)
    }

    def nodeBeforePredicate(row: Row): Boolean = !nodeWindowPredicate(row)

    val nodes = getRowHistories(allRows, "node", nodeCompletePredicate, nodeWindowPredicate, nodeBeforePredicate)
    val nodeIds: Set[Long] = nodes.map(_._1).toSet

    /*********** WAYS ***********/

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

    def wayBeforePredicate(row: Row): Boolean = {
      if (nodeWindowPredicate(row)) false
      else {
        val nds: List[Long] = row.getSeq(6).asInstanceOf[Seq[Row]].map(_.getLong(0)).toList
        nds
          .map({ id => nodes.getOrElse(id, RowHistory(None, None)) })
          .forall({ row => row.beforeWindow != None })
      }
    }

    val ways = getRowHistories(allRows, "way", wayCompletePredicate, wayWindowPredicate, wayBeforePredicate)
    val wayIds: Set[Long] = ways.map(_._1).toSet

    /*********** RELATIONS ***********/

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

    def relCompletePredicate(row: Row): Boolean = {
      val members: List[Row] = row.getSeq(7).asInstanceOf[Seq[Row]].toList
      val nodeMembers = members.filter(_.getString(0) == "node").map(_.getLong(1))
      val wayMembers = members.filter(_.getString(0) == "way").map(_.getLong(1))
      val relMembers = members.filter(_.getString(0) == "relation").map(_.getLong(1))
      val nodesOkay = nodeMembers.forall({ id => nodeIds.contains(id) })
      val waysOkay = wayMembers.forall({ id => wayIds.contains(id) })
      val relsOkay = relMembers.forall({ id => relationIds.contains(id) })
      nodesOkay && waysOkay && relsOkay
    }

    def relWindowPredicate(row: Row): Boolean = {
      if (nodeWindowPredicate(row)) true
      else {
        val members: List[Row] = row.getSeq(7).asInstanceOf[Seq[Row]].toList
        val nodeMembers = members.filter(_.getString(0) == "node").map(_.getLong(1))
        val wayMembers = members.filter(_.getString(0) == "way").map(_.getLong(1))
        val relMembers = members
          .filter(_.getString(0) == "relation")
          .map(_.getLong(1))
          .map({ id => _relations.getOrElse(id, throw new Exception("Oh no")) })
        val nodesYes = nodeMembers
          .map({ id => nodes.getOrElse(id, RowHistory(None, None)) })
          .exists({ row => row.inWindow != None })
        val waysYes = wayMembers
          .map({ id => ways.getOrElse(id, RowHistory(None, None)) })
          .exists({row => row.inWindow != None })
        val relsYes = relMembers.exists({ row => relWindowPredicate(row) })
        nodesYes || waysYes || relsYes
      }
    }

    def relBeforePredicate(row: Row): Boolean = {
      if (nodeWindowPredicate(row)) false
      else {
        val members: List[Row] = row.getSeq(7).asInstanceOf[Seq[Row]].toList
        val nodeMembers = members.filter(_.getString(0) == "node").map(_.getLong(1))
        val wayMembers = members.filter(_.getString(0) == "way").map(_.getLong(1))
        val relMembers = members
          .filter(_.getString(0) == "relation")
          .map(_.getLong(1))
          .map({ id => _relations.getOrElse(id, throw new Exception("Oh no")) })
        val nodesYes = nodeMembers
          .map({ id => nodes.getOrElse(id, RowHistory(None, None)) })
          .forall({ row => row.beforeWindow != None })
        val waysYes = wayMembers
          .map({ id => ways.getOrElse(id, RowHistory(None, None)) })
          .forall({row => row.beforeWindow != None })
        val relsYes = relMembers.forall({ row => relBeforePredicate(row) })
        nodesYes && waysYes && relsYes
      }
    }

    val relations = getRowHistories(allRows, "relation", relCompletePredicate, relWindowPredicate, relBeforePredicate)

    /*********** RENDERING ***********/

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
