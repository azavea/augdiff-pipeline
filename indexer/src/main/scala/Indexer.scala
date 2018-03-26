package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.rocksdb


object Indexer {

  def main(args: Array[String]): Unit = {
    val spark = Common.sparkSession("Indexer")
    import spark.implicits._

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val osm = spark.read.orc(args(0))
    val nodeToWays = osm
      .filter(col("type") === "way")
      .select(
        explode(col("nds.ref")).as("id"),
        struct(
          unix_timestamp(col("timestamp")).as("valid_from"),
          col("id").as("ptr")
        ).as("ptrs")
    )
    val xToRelations = osm
      .filter(col("type") === "relation")
      .select(
        explode(col("members.ref")).as("id"),
        struct(
          unix_timestamp(col("timestamp")).as("valid_from"),
          col("id").as("ptr")
        ).as("ptrs")
    )
    val pointers = nodeToWays.union(xToRelations)
      .groupBy(col("id"))
      .agg(collect_list(col("ptrs")).as("ptrs"))
    val joined =
      osm.join(pointers, "id")
        .withColumn("valid_from", unix_timestamp(col("timestamp")))

    joined.printSchema

    joined
      .rdd
      .map({ row =>
        val id = BigInt(row.getAs[Long]("id"))
        val valid_from = BigInt(row.getAs[Long]("valid_from"))
        val bigint = (id<<64) + valid_from
        (bigint, row)
      })
      .sortBy(_._1)
      .values
      .mapPartitionsWithIndex({ (index: Int, rows: Iterator[Row]) =>
        val eo = new org.rocksdb.EnvOptions
        val o = new org.rocksdb.Options
        val sst = new org.rocksdb.SstFileWriter(eo, o)
        val bytes = Array[Byte](0, 0, 0, 0, 0, 0, 0, 0)

        sst.open(s"./sst-${index}.dat")
        rows.foreach({ row =>
          val id = BigInt(row.getAs[Long]("id")).toByteArray
          val valid_from = BigInt(row.getAs[Long]("valid_from")).toByteArray
          val key: Array[Byte] = bytes.take(8-id.length) ++ id ++ bytes.take(8-valid_from.length) ++ valid_from
          val value: Array[Byte] = {
            val bos = new java.io.ByteArrayOutputStream
            val oos = new java.io.ObjectOutputStream(bos)
            oos.writeObject(row)
            oos.flush
            val data = bos.toByteArray
            bos.close
            data
          }
          sst.add(
            new org.rocksdb.Slice(key),
            new org.rocksdb.Slice(value)
          )
        })
        sst.finish
        List(true).toIterator
      })
      .count
  }

}
