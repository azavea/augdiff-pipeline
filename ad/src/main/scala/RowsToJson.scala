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

  // Turn the set of augmented diff rows into a collection of
  // histories suitable for rendering geometries after and before the
  // update (the latter rendered as invisible so that they can be
  // deleted).
  //
  // The intent here is: for each entity present in the augmented diff
  // set, get the most recent occurrence that is within the update
  // window (should be only one) and the most recent before the update
  // window, and package them together.
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

  def apply(fos: OutputStream, updateRows: Array[Row], allRows: Array[Row]) = {

    val windowSet = updateRows.toSet

    /*********** NODES ***********/

    // Is the node complete? (Nodes always are.)
    def nodeCompletePredicate(row: Row): Boolean = true

    // Is the node in the update window?
    def nodeWindowPredicate(row: Row): Boolean = windowSet.contains(row)

    // Is the node before the update window?  (Not mutually exclusive with the above.)
    def nodeBeforePredicate(row: Row): Boolean = !nodeWindowPredicate(row)

    val nodes = getRowHistories(allRows, "node", nodeCompletePredicate, nodeWindowPredicate, nodeBeforePredicate)
    val nodeIds: Set[Long] = nodes.map(_._1).toSet

    /*********** WAYS ***********/

    // Is the way complete?  (Does the set of augmented diff rows contain everything needed to render the row [all of the nodes]?)
    def wayCompletePredicate(row: Row): Boolean = {
      val nds: List[Long] = row.get(6) match {
        case nds: Seq[Row] => nds.asInstanceOf[Seq[Row]].map(_.getLong(0)).toList
        case nds: Array[Row] => nds.asInstanceOf[Array[Row]].map(_.getLong(0)).toList
      }
      nds.forall({ id => nodeIds.contains(id) })
    }

    // Is the way in the update window?
    def wayWindowPredicate(row: Row): Boolean = {
      if (nodeWindowPredicate(row)) true
      else {
        val nds: List[Long] = row.get(6) match {
          case nds: Seq[Row] => nds.asInstanceOf[Seq[Row]].map(_.getLong(0)).toList
          case nds: Array[Row] => nds.asInstanceOf[Array[Row]].map(_.getLong(0)).toList
        }
        nds
          .map({ id => nodes.getOrElse(id, RowHistory(None, None)) })
          .exists({ row => row.inWindow != None })
      }
    }

    // Is the way renderable before the update window?
    def wayBeforePredicate(row: Row): Boolean = {
      if (nodeWindowPredicate(row)) false
      else {
        val nds: List[Long] = row.get(6) match {
          case nds: Seq[Row] => nds.asInstanceOf[Seq[Row]].map(_.getLong(0)).toList
          case nds: Array[Row] => nds.asInstanceOf[Array[Row]].map(_.getLong(0)).toList
        }
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

    // Does the augmented diff row-set contain everything needed to render this relation?
    def relCompletePredicate(row: Row): Boolean = {
      val members: List[Row] = row.get(7) match {
        case members: Seq[Row] => members.asInstanceOf[Seq[Row]].toList
        case members: Array[Row] => members.asInstanceOf[Array[Row]].toList
      }
      val nodeMembers = members.filter(_.getString(0) == "node").map(_.getLong(1))
      val wayMembers = members.filter(_.getString(0) == "way").map(_.getLong(1))
      val relMembers = members.filter(_.getString(0) == "relation").map(_.getLong(1))
      val nodesOkay = nodeMembers.forall({ id => nodeIds.contains(id) })
      val waysOkay = wayMembers.forall({ id => wayIds.contains(id) })
      val relsOkay = relMembers.forall({ id => relationIds.contains(id) })
      nodesOkay && waysOkay && relsOkay
    }

    // Is the relation part of the update window?
    def relWindowPredicate(row: Row): Boolean = {
      if (nodeWindowPredicate(row)) true
      else {
        val members: List[Row] = row.get(7) match {
          case members: Seq[Row] => members.asInstanceOf[Seq[Row]].toList
          case members: Array[Row] => members.asInstanceOf[Array[Row]].toList
        }
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

    // Can the relation be rendered before the update window?
    def relBeforePredicate(row: Row): Boolean = {
      if (nodeWindowPredicate(row)) false
      else {
        val members: List[Row] = row.get(7) match {
          case members: Seq[Row] => members.asInstanceOf[Seq[Row]].toList
          case members: Array[Row] => members.asInstanceOf[Array[Row]].toList
        }
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

    // Renderable metadata
    def getMetadata(row: Row, visible: Option[Boolean] = None): Map[String, String] = {
      Map(
        "id" -> row.getLong(1).toString,
        "type" -> row.getString(2),
        "tags" -> row.getMap(3).asInstanceOf[Map[String, String]].toJson.toString,
        "changeset" -> row.getLong(8).toString,
        "timestamp" -> row.getTimestamp(9).toString,
        "uid" -> row.getLong(10).toString,
        "user" -> row.getString(11),
        "version" -> row.getLong(12).toString,
        "visible" -> (visible match {
          case None => row.getBoolean(13).toString
          case Some(visible) => visible.toString
        })
      )
    }

    // Renderable geometry
    def getGeometry(row: Row, inWindow: Boolean = true): Geometry = {
      row.getString(2) match {
        case "node" =>
          Point(row.getDecimal(5).doubleValue(), row.getDecimal(4).doubleValue())
        case "way" =>
          val nds: Array[Long] = row.get(6) match {
            case nds: Seq[Row] => nds.asInstanceOf[Seq[Row]].map({ row => row.getLong(0) }).toArray
            case nds: Array[Row] => nds.asInstanceOf[Array[Row]].map({ row => row.getLong(0) }).toArray
          }
          val points = nds
            .map({ id =>
              val row = nodes.get(id).get
              (inWindow, row) match {
                case (true, RowHistory(Some(inWindow), _)) => inWindow
                case (true, RowHistory(None, Some(beforeWindow))) => beforeWindow
                case (false, RowHistory(_, Some(beforeWindow))) => beforeWindow
                case _ => throw new Exception("Oh no")
              }
            })
            .map({ row => Point(row.getDecimal(5).doubleValue(), row.getDecimal(4).doubleValue()) })
          if (nds.head == nds.last) Polygon(points); else Line(points)
        case "relation" =>
          val _members: Array[Row] = row.get(7) match {
            case members: Seq[Row] => members.asInstanceOf[Seq[Row]].toArray
            case members: Array[Row] => members.asInstanceOf[Array[Row]].toArray
          }
          val members = _members.map({ member =>
            val tipe = member.getString(0)
            val id = member.getLong(1)
            val row = tipe match {
              case "node" => nodes.get(id).get
              case "way" => ways.get(id).get
              case "relation" => relations.get(id).get
              case _ => throw new Exception("Oh no")
            }
            (inWindow, row) match {
              case (true, RowHistory(Some(inWindow), _)) => inWindow
              case (true, RowHistory(None, Some(beforeWindow))) => beforeWindow
              case (false, RowHistory(_, Some(beforeWindow))) => beforeWindow
              case _ => throw new Exception("Oh no")
            }
          })
          val geoms = members.map({ row => getGeometry(row, inWindow = inWindow) })
          GeometryCollection(geoms)
      }
    }

    // Open JSON file
    val p = new java.io.PrintWriter(fos)

    // Render to JSON
    (nodes ++ ways ++ relations).foreach({ case (id: Long, row: RowHistory) =>
      row match {
        case RowHistory(Some(inWindow), Some(beforeWindow)) => // delete, modify
          val visibleNow = inWindow.getBoolean(13)
          val p1 =
            if (visibleNow) getGeometry(inWindow, inWindow = true)
            else getGeometry(beforeWindow, inWindow = false)
          val p2 = getGeometry(beforeWindow, inWindow = false)
          val m1 =
            if (visibleNow) getMetadata(inWindow)
            else getMetadata(beforeWindow, visible = Some(false))
          val m2 =
            if (visibleNow) getMetadata(beforeWindow, visible = Some(false))
            else getMetadata(beforeWindow)

          p.write(Feature(p1, m1).toJson.toString + "\n")
          if (visibleNow) {
            p.write(Feature(p2, m2).toJson.toString + "\n")
          }
        case RowHistory(Some(inWindow), None) => // create
          if (inWindow.getBoolean(13)) {
            val p1 = getGeometry(inWindow, inWindow = true)
            val m1 = getMetadata(inWindow)
            p.write(Feature(p1, m1).toJson.toString + "\n")
          }
        case _ =>
      }
    })

    // Close JSON file
    p.flush
    p.close

  }

}
