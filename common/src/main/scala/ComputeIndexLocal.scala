package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable


object ComputeIndexLocal {
  private val logger = {
    val logger = Logger.getLogger(this.getClass)
    logger.setLevel(Level.INFO)
    logger
  }

  private def edgesFromRows(rows: Array[Row]): Array[Row] = {
    val loops: Array[Row] =
      rows
        .flatMap({ r =>
          val id = r.getLong(1)                       /* id */
          val tipe = r.getString(2)                   /* type */
          val p = Common.partitionNumberFn(id, tipe)
          val instant = r.getTimestamp(9).getTime     /* timestamp */
          Array[Row](
            Row(p, id, tipe, instant, p, id, tipe, true),
            Row(p, id, tipe, instant, p, id, tipe, false)
          )
        })
    val halfEdgesFromNodes: Array[Row] =
      rows
        .filter({ r => r.getString(2) /* type */ == "way" })
        .flatMap({ r =>
          val bid = r.getLong(1)                                  /* id */
          val btype = r.getString(2)                              /* type */
          val bp = Common.partitionNumberFn(bid, btype)
          val instant = r.getTimestamp(9).getTime                 /* timestamp */
          val nds: Array[Row] = r.get(6).asInstanceOf[Array[Row]] /* nds */

          nds.flatMap({ nd =>
            val aid = nd.getLong(0)
            val atype = "node"
            val ap = Common.partitionNumberFn(aid, atype)
            val forward = Row(
              ap, aid, atype,
              instant,
              bp, bid, btype,
              true
            )
            val reverse = Row(
              bp, bid, btype,
              instant,
              ap, aid, atype,
              false
              )
            Array[Row](forward, reverse)
          })
        })
    val halfEdgesFromRelations: Array[Row] =
      rows
        .filter({ r => r.getString(2) /* type */ == "relation" })
        .flatMap({ r =>
          val bid = r.getLong(1)                                      /* id */
          val btype = r.getString(2)                                  /* type */
          val bp = Common.partitionNumberFn(bid, btype)
          val instant = r.getTimestamp(9).getTime                     /* timestamp */
          val members: Array[Row] = r.get(7).asInstanceOf[Array[Row]] /* members */

          members.flatMap({ member =>
            val atype = member.getString(0) /* members.type */
            val aid = member.getLong(1)  /* members.ref */
            val ap = Common.partitionNumberFn(aid, atype)
            val forward = Row(
              ap, aid, atype,
              instant,
              bp, bid, btype,
              true
            )
            val reverse = Row(
              bp, bid, btype,
              instant,
              ap, aid, atype,
              false
            )
            Array[Row](forward, reverse)
          })
        })

    (loops ++ halfEdgesFromNodes ++ halfEdgesFromRelations).distinct
  }

  private def transitiveStep(
    leftEdges: mutable.Set[Row],
    rightEdges: Map[(Long, String), Array[Row]],
    iteration: Long
  ) = {
    logger.info(s"◼ Transitive closure iteration=$iteration left=${leftEdges.size}")
    leftEdges
      .flatMap({ row1 => // Manual inner join
        val leftAp = row1.getLong(0)           /* ap */
        val leftAid = row1.getLong(1)          /* aid */
        val leftAtype = row1.getString(2)      /* atype */
        val leftInstant = row1.getLong(3)      /* instant */
        val leftBp = row1.getLong(4)           /* bp */
        val leftBid = row1.getLong(5)          /* bid */
        val leftBtype = row1.getString(6)      /* btype */
        val leftDirection = row1.getBoolean(7) /* a_to_b */
        val key = (leftBid, leftBtype)

        rightEdges.getOrElse(key, Array.empty[Row])
          .flatMap({ row2 =>
            val rightAp = row2.getLong(0)           /* ap */
            val rightAid = row2.getLong(1)          /* aid */
            val rightAtype = row2.getString(2)      /* atype */
            val rightInstant = row2.getLong(3)      /* instant */
            val rightBp = row2.getLong(4)           /* bp */
            val rightBid = row2.getLong(5)          /* bid */
            val rightBtype = row2.getString(6)      /* btype */
            val rightDirection = row2.getBoolean(7) /* a_to_b */

            if (leftBid != rightAid || leftBtype != rightAtype) None // The two edges must meet
            else if (leftDirection != rightDirection) None // The edges must go in the same direction
            else if (leftAtype != "relation" && rightBtype != "relation") None // Extended chains are over relations
            else if (leftAid == rightBid && leftAtype == rightBtype) None // Do not join thing to itself
            else {
              Some(Row(
                leftAp, leftAid, leftAtype,
                math.max(leftInstant, rightInstant),
                rightBp, rightBid, rightBtype,
                leftDirection
              ))
            }
          })
      })
  }

  def apply(
    rows: Array[Row],
    leftEdgesDf: DataFrame
  ): DataFrame = {
    logger.info(s"◼ Computing Index")

    val rightEdges: Array[Row] = edgesFromRows(rows)
    val rightRelationEdgesMap: Map[(Long, String), Array[Row]] =
      rightEdges
        .filter({ row => row.getString(6) /* btype */ == "relation" })
        .groupBy({ row => (row.getLong(1) /* aid */, row.getString(2) /* atype*/) })
    val desired = rightEdges.map({ r => (r.getLong(1) /* aid */, r.getString(2) /* atype */) }).toSet
    val outputEdges: mutable.Set[Row] = (mutable.Set.empty[Row] ++ rightEdges)
    val leftEdges: mutable.Set[Row] = (OrcBackend.loadEdges(desired, leftEdgesDf) ++= rightEdges)
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

    val schema = leftEdgesDf.select(Common.edgeColumns: _*).schema
    val spark = leftEdgesDf.sparkSession
    val sc = spark.sparkContext

    spark.createDataFrame(sc.parallelize(outputEdges.toSeq, 1), schema)
  }

}
