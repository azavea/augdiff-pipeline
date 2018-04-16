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
              bp, bid, btype
            )
            val reverse = Row(
              bp, bid, btype,
              instant,
              ap, aid, atype
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
              bp, bid, btype
            )
            val reverse = Row(
              bp, bid, btype,
              instant,
              ap, aid, atype
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
                rightBp, rightBid, rightBtype
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
    val rightEdgesMap: Map[(Long, String), Array[Row]] =
      rightEdges
        .groupBy({ row =>
          (row.getLong(1) /* aid */, row.getString(2) /* atype*/)
        })
    var outputEdges: Array[Row] = rightEdges
    var leftEdges: Array[Row] = {
      val groupSize = 150
      val desired = rightEdges
        .map({ r => (r.getLong(1) /* aid */, r.getString(2) /* atype */) })
        .toSet
      val pairs = desired
        .groupBy({ pair => Common.partitionNumberFn(pair._1, pair._2) })
      logger.info(s"◼ Reading ${pairs.size} partitions in groups of ${groupSize}") // 175 bug (groupSize <= 175)
      val dfs = pairs.grouped(groupSize).toList.map({ group =>
        logger.info("◼ Reading group")
        val ps = group.map({ kv => kv._1 }).toArray
        val ids = group.flatMap({ kv => kv._2.map(_._1) }).toArray.distinct
        val retval = leftEdgesDf
          .filter(col("bp").isin(ps: _*)) // partition pruning
        if (ids.length < 4096)
          retval.filter(col("bid").isin(ids: _*)) // predicate pushdown
        else retval
      })
      dfs.map({ df =>
        df.select(Common.edgeColumns: _*)
          .collect
          .filter({ r => desired.contains((r.getLong(1) /* aid */, r.getString(2) /* atype */)) })
      }).reduce(_ ++ _) ++ rightEdges
    }
    var iteration = 1L
    var keepGoing = false

    do {
      val newEdges = transitiveStep(leftEdges, rightEdgesMap, iteration)
      leftEdges = (leftEdges ++ newEdges).distinct
      val before = outputEdges.length
      outputEdges = (outputEdges ++ newEdges).distinct
      val after = outputEdges.length
      iteration = iteration + 1
      keepGoing = (before != after)
    } while (keepGoing)

    val schema = leftEdgesDf.select(Common.edgeColumns: _*).schema
    val spark = leftEdgesDf.sparkSession
    val sc = spark.sparkContext

    spark.createDataFrame(sc.parallelize(outputEdges, 1), schema)
  }

}
