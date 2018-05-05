package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable


object ComputeIndexLocal {

  sealed case class Edge(a: Long, b: Long, instant: Long, direction: Boolean)

  private val logger = {
    val logger = Logger.getLogger(this.getClass)
    logger.setLevel(Level.INFO)
    logger
  }

  private def edgesFromRows(rows: Array[Row]): Array[Edge] = {
    rows.flatMap({ r =>
      val bId = r.getLong(1)      /* id */
      val bType = r.getString(2)  /* type */
      val b = Common.pairToLongFn(bId, bType)

      tipe match {
        case "node" =>
          Array[Edge](
            Edge(a = b, b = b, instant = instant, direction = true),
            Edge(a = b, b = b, instant = instant, direction = false))
        case "way" =>
          val nds: Array[Row] = r.get(6).asInstanceOf[Array[Row]] /* nds */
          nds.flatMap({ nd =>
            val id = nd.getLong(0)
            val tipe = "node"
            val a = Common.pairToLongFn(id, tipe)
            Array[Edge](
              Edge(a = a, b = b, instant = instant, direction = true),
              Edge(a = b, b = a, instant = instant, direction = false))
          })
        case "relation" =>
          val members: Array[Row] = r.get(7).asInstanceOf[Array[Row]] /* members */
          members.flatMap({ member =>
            val tipe = member.getString(0) /* members.type */
            val id = member.getLong(1)     /* members.ref */
            val a = Common.pairToLongFn(id, tipe)
            Array[Edge](
              Edge(a = a, b = b, instant = instant, direction = true),
              Edge(a = b, b = a, instant = instant, direction = false))
          })
      }
    })
  }

  private def transitiveStep(
    leftEdges: mutable.Set[Edge],
    rightEdges: Map[Long, Array[Edge]],
    iteration: Long
  ) = {
    logger.info(s"◼ Transitive closure iteration=$iteration left=${leftEdges.size}")
    leftEdges.flatMap({ left: Edge => // Manual inner join
        val leftAtype = Common.longToTypeFn(left.a)

        rightEdges.getOrElse(left.b, Array.empty[Edge]).flatMap({ right: Edge =>
          val rightBtype = Common.longToTypeFn(right.b)

            if (left.b != right.a || left.b != right.a) None                   // The two directed edges must meet
            else if (left.direction  != right.direction) None                  // The edges must go in the same direction
            else if (leftAtype != "relation" && rightBtype != "relation") None // Extended chains are over relations
            else if (left.a == right.b) None                                   // Do not join thing to itself
            else {
              Some(Edge(
                a = left.a,
                b = right.b,
                instant = math.max(left.instant, right.instant),
                direction = left.direction))
            }
          })
      })
  }

  def apply(
    rows: Array[Row],
    uri: String, props: java.util.Properties
  ): Set[Edge] = {
    logger.info(s"◼ Computing Index")

    val rightEdges: Array[Edge] = edgesFromRows(rows)
    val rightRelationEdgesMap: Map[Long, Array[Edge]] = rightEdges
      .filter({ edge => Common.longToTypeFn(edge.b) == "relation" })
      .groupBy(_.a)
    val desired: Set[Long] = rightEdges.map(_.a).toSet
    val outputEdges: mutable.Set[Edge] = (mutable.Set.empty[Edge] ++= rightEdges)
    val leftEdges: mutable.Set[Edge] = (PostgresBackend.loadEdges(desired, uri, props) ++= rightEdges)
    var iteration = 1L
    var keepGoing = false

    do {
      val newEdges = transitiveStep(leftEdges, rightRelationEdgesMap, iteration)
      leftEdges ++= newEdges
      val before = outputEdges.size
      outputEdges ++= newEdges
      val after = outputEdges.size
      iteration = iteration + 1
      keepGoing = (before != after)
    } while (keepGoing)

    outputEdges.toSet
  }

}
