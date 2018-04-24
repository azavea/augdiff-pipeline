package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable


object ComputeIndexLocal {

  sealed case class Edge(a: Long, b: Long, direction: Boolean)

  private val logger = {
    val logger = Logger.getLogger(this.getClass)
    logger.setLevel(Level.INFO)
    logger
  }

  private def edgesFromRows(rows: Array[Row]): Array[Edge] = {
    rows.flatMap({ r =>
      val id = r.getLong(1)     /* id */
      val tipe = r.getString(2) /* type */
      val b = Common.pairToLongFn(id, tipe)

      tipe match {
        case "node" =>
          Array[Edge](Edge(b, b, true), Edge(b, b, false))
        case "way" =>
          val nds: Array[Row] = r.get(6).asInstanceOf[Array[Row]] /* nds */
          nds.flatMap({ nd =>
            val id = nd.getLong(0)
            val tipe = "node"
            val a = Common.pairToLongFn(id, tipe)
            Array[Edge](Edge(a, b, true), Edge(b, a, false))
          })
        case "relation" =>
          val members: Array[Row] = r.get(7).asInstanceOf[Array[Row]] /* members */
          members.flatMap({ member =>
            val tipe = member.getString(0) /* members.type */
            val id = member.getLong(1)     /* members.ref */
            val a = Common.partitionNumberFn(id, tipe)
            Array[Edge](Edge(a, b, true), Edge(b, a, false))
          })
      }
    })
  }

  // private def transitiveStep(
  //   leftEdges: mutable.Set[Row],
  //   rightEdges: Map[(Long, String), Array[Row]],
  //   iteration: Long
  // ) = {
  //   logger.info(s"◼ Transitive closure iteration=$iteration left=${leftEdges.size}")
  //   leftEdges
  //     .flatMap({ row1 => // Manual inner join
  //       val leftAp = row1.getLong(0)           /* ap */
  //       val leftAid = row1.getLong(1)          /* aid */
  //       val leftAtype = row1.getString(2)      /* atype */
  //       val leftInstant = row1.getLong(3)      /* instant */
  //       val leftBp = row1.getLong(4)           /* bp */
  //       val leftBid = row1.getLong(5)          /* bid */
  //       val leftBtype = row1.getString(6)      /* btype */
  //       val leftDirection = row1.getBoolean(7) /* a_to_b */
  //       val key = (leftBid, leftBtype)

  //       rightEdges.getOrElse(key, Array.empty[Row])
  //         .flatMap({ row2 =>
  //           val rightAp = row2.getLong(0)           /* ap */
  //           val rightAid = row2.getLong(1)          /* aid */
  //           val rightAtype = row2.getString(2)      /* atype */
  //           val rightInstant = row2.getLong(3)      /* instant */
  //           val rightBp = row2.getLong(4)           /* bp */
  //           val rightBid = row2.getLong(5)          /* bid */
  //           val rightBtype = row2.getString(6)      /* btype */
  //           val rightDirection = row2.getBoolean(7) /* a_to_b */

  //           if (leftBid != rightAid || leftBtype != rightAtype) None // The two edges must meet
  //           else if (leftDirection != rightDirection) None // The edges must go in the same direction
  //           else if (leftAtype != "relation" && rightBtype != "relation") None // Extended chains are over relations
  //           else if (leftAid == rightBid && leftAtype == rightBtype) None // Do not join thing to itself
  //           else {
  //             Some(Row(
  //               leftAp, leftAid, leftAtype,
  //               math.max(leftInstant, rightInstant),
  //               rightBp, rightBid, rightBtype,
  //               leftDirection
  //             ))
  //           }
  //         })
  //     })
  // }

  def apply(
    rows: Array[Row],
    uri: String, props: java.util.Properties
  ): Set[Edge] = {
    logger.info(s"◼ Computing Index")

    val rightEdges: Array[Edge] = edgesFromRows(rows)
    val rightRelationEdgesmap: Map[Long, Array[Edge]] = rightEdges
      .filter({ edge => Common.longToTypeFn(edge.b) == "relation" })
      .groupBy(_.a)
    val desired: Set[Long] = rightEdges.map(_.a).toSet
    val outputEdges: mutable.Set[Edge] = (mutable.Set.empty[Edge] ++= rightEdges)
    val leftEdges: mutable.Set[Edge] = (PostgresBackend.loadEdges(desired, uri, props) ++= rightEdges)
    // var iteration = 1L
    // var keepGoing = false

    // do {
    //   val newEdges = transitiveStep(leftEdges, rightRelationEdgesMap, iteration)
    //   leftEdges ++= newEdges
    //   val before = outputEdges.size
    //   outputEdges ++= newEdges
    //   val after = outputEdges.size
    //   iteration = iteration + 1
    //   keepGoing = (before != after)
    // } while (keepGoing)

    // val schema = leftEdgesDf.select(Common.edgeColumns: _*).schema
    // val spark = leftEdgesDf.sparkSession
    // val sc = spark.sparkContext

    outputEdges.toSet
  }

}
