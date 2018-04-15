package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

// import scala.collection.JavaConversions._
// import scala.collection.mutable.ArrayBuffer


object ComputeIndexLocal {
  private val logger = {
    val logger = Logger.getLogger(this.getClass)
    logger.setLevel(Level.INFO)
    logger
  }

  private def edgesFromRows(rows: Array[Row]): Array[Row] = {
    val halfEdgesFromNodes: Array[Row] =
      rows
        .filter({ r => r.getString(2) /* type */ == "way" })
        .flatMap({ r =>
          val bid = r.getLong(1) /* id */
          val btype = r.getString(2) /* type */
          val bp = Common.partitionNumberFn(bid, btype)
          val instant = r.getTimestamp(9).getTime /* timestamp */
          val nds: Array[Row] = r.get(6).asInstanceOf[Array[Row]] /* nds */

          nds.flatMap({ nd =>
            val aid = nd.getLong(0)
            val atype = "node"
            val ap = Common.partitionNumberFn(aid, atype)
            val forward = Row(
                ap, aid, atype,
                instant,
                bp, bid, btype,
                0L, /* XXX iteration */
                false
              )
            val reverse = Row(
                bp, bid, btype,
                instant,
                ap, aid, atype,
                0L, /* XXX iteration */
                true
              )
            Array[Row](forward, reverse)
          })
        })
    val halfEdgesFromRelations: Array[Row] =
      rows
        .filter({ r => r.getString(2) /* type */ == "relation" })
        .flatMap({ r =>
          val bid = r.getLong(1) /* id */
          val btype = r.getString(2) /* type */
          val bp = Common.partitionNumberFn(bid, btype)
          val instant = r.getTimestamp(9).getTime /* timestamp */
          val members: Array[Row] = r.get(7).asInstanceOf[Array[Row]] /* members */

          members.flatMap({ member =>
            val atype = member.getString(0) /* members.type */
            val aid = member.getLong(1)  /* members.ref */
            val ap = Common.partitionNumberFn(aid, atype)
            val forward = Row(
              ap, aid, atype,
              instant,
              bp, bid, btype,
              0L, /* XXX iteration */
              false
            )
            val reverse = Row(
              bp, bid, btype,
              instant,
              ap, aid, atype,
              0L, /* XXX iteration */
              true
            )
            Array[Row](forward, reverse)
          })
        })

    (halfEdgesFromNodes ++ halfEdgesFromRelations).distinct
  }

  def transitiveStep(
    leftEdges: Array[Row],
    rightEdges: Map[(Long, String), Array[Row]],
    iteration: Long
  ): Array[Row] = {
    logger.info(s"◼ Transitive closure iteration=$iteration left=${leftEdges.length}")
    leftEdges
      .flatMap({ row1 => // Manual inner join
        val leftAp = row1.getLong(0)      /* ap */
        val leftAid = row1.getLong(1)     /* aid */
        val leftAtype = row1.getString(2) /* atype */
        val leftInstant = row1.getLong(3) /* instant */
        val leftBp = row1.getLong(4)      /* bp */
        val leftBid = row1.getLong(5)     /* bid */
        val leftBtype = row1.getString(6) /* btype */
        val key = (leftBid, leftBtype)

        rightEdges.getOrElse(key, Array.empty[Row])
          .flatMap({ row2 =>
            val rightAp = row2.getLong(0)      /* ap */
            val rightAid = row2.getLong(1)     /* aid */
            val rightAtype = row2.getString(2) /* atype */
            val rightInstant = row2.getLong(3) /* instant */
            val rightBp = row2.getLong(4)      /* bp */
            val rightBid = row2.getLong(5)     /* bid */
            val rightBtype = row2.getString(6) /* btype */

            if (leftBid != rightAid || leftBtype != rightAtype) None // The two edges must meet
            else if (leftAtype == "way" && rightBtype == "way") None // Do not join way to way
            else if (leftAtype == "node" && rightBtype == "node") None // Do not join node to node
            else if (leftAid == rightBid && leftAtype == rightBtype) None // Do not join thing to itself
            else {
              Some(Row(
                leftAp, leftAid, leftAtype,
                math.max(leftInstant, rightInstant),
                rightBp, rightBid, rightBtype,
                0L, // XXX
                false
              ))
            }
          })
      }).distinct
  }

  def apply(
    rows: Array[Row],
    previousEdgesDf: DataFrame
  ): DataFrame = {
    logger.info(s"◼ Computing Index")

    val initialEdges: Array[Row] = edgesFromRows(rows)
    val initialEdgesMap: Map[(Long, String), Array[Row]] =
      initialEdges
        .groupBy({ row =>
          (row.getLong(1) /* aid */, row.getString(2) /* atype*/)
        })

    var additionalEdges: Array[Row] = initialEdges
    var previousEdges: Array[Row] = {
      val ps = initialEdges.map({ r => r.getLong(0) /* ap */ }).distinct
      val ids = initialEdges.map({ r => r.getLong(1) /* aid */ }).distinct
      val dfs = ps.grouped(150).map({ list => // 175 bug (150 <= 175)
        previousEdgesDf
          .filter(col("bp").isin(list: _*)) // partition pruning
          .filter(col("bid").isin(ids: _*)) // predicate pushdown
      })
      dfs.map({ df => df.select(Common.edgeColumns: _*).collect }).reduce(_ ++ _) ++ initialEdges
    }
    var iteration = 1L
    var keepGoing = false

    do {
      val newEdges = transitiveStep(previousEdges, initialEdgesMap, iteration)
      previousEdges = (previousEdges ++ newEdges).distinct
      val before = additionalEdges.length
      additionalEdges = (additionalEdges ++ newEdges).distinct
      val after = additionalEdges.length
      iteration = iteration + 1
      keepGoing = (before != after)
    } while (keepGoing)

    val schema = previousEdgesDf.select(Common.edgeColumns: _*).schema
    val spark = previousEdgesDf.sparkSession
    val sc = spark.sparkContext

    spark.createDataFrame(sc.parallelize(additionalEdges, 1), schema)
  }

}
