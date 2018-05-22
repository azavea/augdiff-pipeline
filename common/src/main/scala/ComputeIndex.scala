package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel


object ComputeIndex {

  /**
    * Push dependency sets backwards through the graph.
    */
  private def pushBackwards(graph: Graph[Any, Any]): Graph[Set[VertexId], Any] = {

    def vprog(id: VertexId, current: Set[VertexId], msg: Set[VertexId]): Set[VertexId] =
      (current ++ msg + id)

    def sendMessage(edge: EdgeTriplet[Set[VertexId], Any]): Iterator[(VertexId, Set[VertexId])] = {
      if (!edge.dstAttr.subsetOf(edge.srcAttr)) Iterator((edge.srcId, edge.dstAttr))
      else Iterator.empty
    }

    def mergeMsg(left: Set[VertexId], right: Set[VertexId]): Set[VertexId] = {
      (left ++ right)
    }

    Pregel(graph.mapVertices({ case (id, _) => Set(id) }),
      Set.empty[VertexId],
      Int.MaxValue,
      EdgeDirection.In
    )(vprog = vprog,
      sendMsg = sendMessage,
      mergeMsg = mergeMsg)
  }

  private val logger = {
    val logger = Logger.getLogger(this.getClass)
    logger.setLevel(Level.INFO)
    logger
  }

  private def edgesFromRows(rows: DataFrame): DataFrame = {
    val halfEdgesFromNodes =
      rows
        .filter(col("type") === "way")
        .select(
          Common.pairToLongUdf(col("id"), col("type")).as("b"),
          explode(col("nds")).as("nds"))
        .select(
          Common.pairToLongUdf(col("nds.ref"), lit("node")).as("a"),
          col("b"))
        .select(Common.edgeColumns: _*)
    val halfEdgesFromRelations =
      rows
        .filter(col("type") === "relation")
        .select(
          Common.pairToLongUdf(col("id"), col("type")).as("b"),
          explode(col("members")).as("members"))
        .select(
          Common.pairToLongUdf(col("members.ref"), col("members.type")).as("a"),
          col("b"))
        .select(Common.edgeColumns: _*)

    halfEdgesFromNodes.union(halfEdgesFromRelations)
  }

  def apply(rows: DataFrame): DataFrame = {
    logger.info(s"◻ Computing Index")

    val edgeDf = edgesFromRows(rows)
    val vertices: RDD[(VertexId, Any)] = edgeDf.rdd.flatMap({
      row => List[(VertexId, Any)]((row.getLong(0), null), (row.getLong(1), null))
    }).distinct
    val edges: RDD[Edge[Any]] = edgeDf.rdd.map({ row =>
      Edge(row.getLong(0), row.getLong(1), null)
    })
    val defaultVertex = (-1L, null)
    val graph = Graph(vertices, edges, defaultVertex)
    val data = pushBackwards(graph)
      .vertices
      .flatMap({ case (a, bs) =>
        bs.map({ b => (a, b) }).filter({ case (a, b) => a != b })
      })

    rows.sparkSession.createDataFrame(
      data.map({ case (a, b) => Row(a, b) }),
      StructType(Common.indexSchema))
  }

}
