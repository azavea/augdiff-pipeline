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

  def saveIndex(
    index: DataFrame,
    uri: String, props: java.util.Properties,
    tableName: String, mode: String
  ): Unit = {
    logger.info(s"Writing index into ${uri}")
    index
      .select(Common.indexColumns: _*)
      .write
      .mode(mode)
      .jdbc(uri, tableName, props)
  }

  def saveIndex(
    edges: Set[ComputeIndexLocal.Edge],
    uri: String, props: java.util.Properties,
    tableName: String
  ): Unit = {
    logger.info(s"Writing index into ${uri}")
    val connection = java.sql.DriverManager.getConnection(uri, props)
    val statement = connection.createStatement

    connection.setAutoCommit(false)
    edges.foreach({ edge =>
      val sql = s"insert into ${tableName} (a, b) values ($edge.a, $edge.b);"
      statement.executeUpdate(sql)
    })
    connection.commit

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
      val subquery = s"select distinct(b) from index where a in ${desired.toString.drop(3)}"
      val query = s"select a, b from index where b in ($subquery)"
      val forward = statement.executeQuery(query)
      while (forward.next) {
        val a = forward.getLong("a")
        val b = forward.getLong("b")
        retval += ComputeIndexLocal.Edge(a = a, b = b)
      }
    })

    statement.close
    connection.close
    retval
  }

}
