package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable


object PostgresBackend {

  private val logger = {
    val logger = Logger.getLogger(this.getClass)
    logger.setLevel(Level.INFO)
    logger
  }

  private val groupLimit = 1024

  def saveBulk(
    bulk: DataFrame,
    uri: String, props: java.util.Properties,
    tableName: String, mode: String
  ): Unit = {
    logger.info(s"Writing OSM into ${uri}")
    bulk
      .write
      .mode(mode)
      .jdbc(uri, tableName, props)
  }

  def saveIndex(
    index: DataFrame,
    uri: String, props: java.util.Properties,
    tableName: String, mode: String
  ): Unit = { // Only store edges from a to b
    logger.info(s"Writing index into ${uri}")
    index
      .filter(col("a_to_b") === true)
      .select(
        Common.pairToLongUdf(col("aid"), col("atype")).as("a"),
        Common.pairToLongUdf(col("bid"), col("btype")).as("b"),
        col("instant"))
      .write
      .mode(mode)
      .jdbc(uri, tableName, props)
  }

  def saveIndex(
    index: Set[ComputeIndexLocal.Edge],
    uri: String, props: java.util.Properties,
    tableName: String
  ): Unit = { // Only store edges from a to b
    logger.info(s"Writing index into ${uri}")
    val connection = java.sql.DriverManager.getConnection(uri, props)
    val statement = connection.createStatement
    val edgeSet: Set[(Long, Long, Long)] = index.map({ edge =>
      if (edge.direction == true) (edge.a, edge.b, edge.instant) // Edge from a to b
      else (edge.b, edge.a, edge.instant)                        // Edge from b to a
    })
    edgeSet.foreach({ case (a, b, instant) =>
      val sql = s"insert into ${tableName} (a, b, instant) values ($a, $b, $instant);"
      statement.executeUpdate(sql)
    })
    statement.close
    connection.close
  }

  def loadEdges(
    desired: Set[Long],
    uri: String, props: java.util.Properties
  ): mutable.Set[ComputeIndexLocal.Edge] = {
    val connection = java.sql.DriverManager.getConnection(uri, props)
    val statement = connection.createStatement
    val retval = mutable.Set.empty[ComputeIndexLocal.Edge]

    desired.grouped(groupLimit).foreach({ desired =>
      val forward = statement.executeQuery(s"select a, b, instant from index where b in ${desired.toString.drop(3)}")
      while (forward.next) {
        val a = forward.getLong("a")
        val b = forward.getLong("b")
        val instant = forward.getLong("instant")
        retval += ComputeIndexLocal.Edge(a = a, b = b, instant = instant, direction = true)
      }

      val backward = statement.executeQuery(s"select a, b, instant from index where a in ${desired.toString.drop(3)}")
      while (backward.next) {
        val a = backward.getLong("a")
        val b = backward.getLong("b")
        val instant = backward.getLong("instant")
        retval += ComputeIndexLocal.Edge(a = b, b = a, instant = instant, direction = false)
      }
    })

    statement.close
    connection.close
    retval
  }

}
