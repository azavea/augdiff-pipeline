package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable


object ComputeIndexLocal {

  sealed case class Edge(a: Long, b: Long)

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

      bType match {
        case "node" =>
          Array[Edge](Edge(a = b, b = b))
        case "way" =>
          val nds: Array[Row] = r.get(6).asInstanceOf[Array[Row]] /* nds */
          nds.flatMap({ nd =>
            val aId = nd.getLong(0)
            val aType = "node"
            val a = Common.pairToLongFn(aId, aType)
            Array[Edge](Edge(a = a, b = b))
          })
        case "relation" =>
          val members: Array[Row] = r.get(7).asInstanceOf[Array[Row]] /* members */
          members.flatMap({ member =>
            val aId = member.getLong(1)     /* members.ref */
            val aType = member.getString(0) /* members.type */
            val a = Common.pairToLongFn(aId, aType)
            Array[Edge](Edge(a = a, b = b))
          })
      }
    })
  }

  def apply(
    rows: Array[Row],
    uri: String, props: java.util.Properties
  ): (Set[Edge], Set[Edge]) = {
    logger.info(s"◼ Computing Index")

    val rowEdges: Set[Edge] = edgesFromRows(rows).toSet
    val bs: Set[Long] = rowEdges.map({ edge => edge.b }).toSet
    val existingEdges: Set[Edge] = PostgresBackend.loadEdges(bs, uri, props).toSet

    // Collect dependencies
    val graph: Map[Long, mutable.Set[Long]] = (rowEdges ++ existingEdges)
      .groupBy({ edge => edge.a })
      .map({ case (a, edges) => (a, (mutable.Set.empty[Long] ++ edges.map(_.b))) })
    var keepGoing = false
    do {
      keepGoing = false
      graph.foreach({ case (a, dependencies1) =>
        dependencies1.foreach({ b =>
          graph.get(b) match {
            case Some(dependencies2: mutable.Set[Long]) =>
              if (!dependencies2.subsetOf(dependencies1)) {
                dependencies1 ++= dependencies2
                keepGoing = true
              }
            case None =>
          }
        })
      })
    } while (keepGoing)

    // Gather all edges, compute new edges
    val allEdges: Set[Edge] = graph
      .flatMap({ case (a, dependencies) => dependencies.map({ b => Edge(a = a, b = b) }) })
      .filter({ case Edge(a, b) => a != b })
      .toSet
    val newEdges: Set[Edge] = allEdges
      .diff(existingEdges)

    (newEdges, allEdges)
  }

}
