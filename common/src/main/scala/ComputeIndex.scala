package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object ComputeIndex {

  private val logger = {
    val logger = Logger.getLogger(this.getClass)
    logger.setLevel(Level.INFO)
    logger
  }

  private def mirror(edges: DataFrame): DataFrame = {
    edges
      .select(
        col("bp").as("ap"), col("bid").as("aid"), col("btype").as("atype"),
        col("instant"),
        col("ap").as("bp"), col("aid").as("bid"), col("atype").as("btype"))
  }

  private def edgesFromRows(rows: DataFrame): DataFrame = {
    val halfEdgesFromNodes =
      rows
        .filter(col("type") === "way")
        .select(
          col("id").as("bid"),
          col("type").as("btype"),
          Common.getInstant(col("timestamp")).as("instant"),
          explode(col("nds")).as("nds"))
        .select(
          Common.partitionNumberUdf(col("nds.ref"), lit("node")).as("ap"),
          col("nds.ref").as("aid"), lit("node").as("atype"),
          col("instant"),
          Common.partitionNumberUdf(col("bid"), col("btype")).as("bp"),
          col("bid"), col("btype"),
          lit(0L).as("iteration"))
    val halfEdgesFromRelations =
      rows
        .filter(col("type") === "relation")
        .select(
          col("id").as("bid"),
          col("type").as("btype"),
          Common.getInstant(col("timestamp")).as("instant"),
          explode(col("members")).as("members"))
        .select(
          Common.partitionNumberUdf(col("members.ref"), col("members.type")).as("ap"),
          col("members.ref").as("aid"),
          col("members.type").as("atype"),
          col("instant"),
          Common.partitionNumberUdf(col("bid"), col("btype")).as("bp"),
          col("bid"), col("btype"),
          lit(0L).as("iteration"))

    halfEdgesFromNodes.union(halfEdgesFromRelations)
  }

  private def transitiveStep(
    leftEdges: DataFrame, rightEdges: DataFrame, iteration: Long
  ): DataFrame = {
    logger.info(s"◻ Transitive closure iteration=$iteration")
    leftEdges
      .filter(col("iteration") === iteration-1)
      .as("left")
      .join(
      rightEdges.as("right"),
        ((col("left.bp") === col("right.ap")) && // Try to use partition pruning (may get better in some future version)
          (col("left.bid") === col("right.aid") && col("left.btype") === col("right.atype")) && // The two edges meet
          (col("left.atype") =!= lit("way") || col("right.btype") =!= lit("way")) && // Do no join way to way
          (col("left.atype") =!= lit("node") || col("right.btype") =!= lit("node")) && // Do no join node to node
          (col("left.aid") =!= col("right.bid") || col("left.atype") =!= col("right.btype"))), // Do not join something to itself
        "inner")
      .select(
        col("left.ap").as("ap"), col("left.aid").as("aid"), col("left.atype").as("atype"),
        Common.larger(col("left.instant"), col("right.instant")).as("instant"),
        col("right.bp").as("bp"), col("right.bid").as("bid"), col("right.btype").as("btype"),
        lit(iteration).as("iteration"))
  }

  def apply(rows: DataFrame): DataFrame = {
    logger.info(s"◻ Computing Index")

    val rightEdges = edgesFromRows(rows).select(Common.edgeColumnsPlus: _*)
    var outputEdges = rightEdges
    var leftEdges = rightEdges
    var iteration = 1L
    var keepGoing = false

    do {
      val newEdges = transitiveStep(leftEdges, rightEdges, iteration).select(Common.edgeColumnsPlus: _*)
      leftEdges = leftEdges.union(newEdges).select(Common.edgeColumnsPlus: _*)
      outputEdges = outputEdges.union(newEdges).select(Common.edgeColumnsPlus: _*)
      iteration = iteration + 1L
      keepGoing = (iteration < 7) && (!newEdges.rdd.isEmpty)
    } while (keepGoing)

    outputEdges.select(Common.edgeColumns: _*)
      .union(mirror(outputEdges).select(Common.edgeColumns: _*))
  }

}
