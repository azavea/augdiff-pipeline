package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.JavaConversions._

import org.rocksdb


object AugmentedDiff {

  private val dbDirectory = "./database"
  private val sstDirectory = "./sst"

  private def files(directory: String) = {
    val list: List[String] = (new java.io.File(directory))
      .listFiles
      .filter(_.isFile)
      .map(_.getAbsolutePath)
      .toList
    list
  }

  def main(args: Array[String]): Unit = {
    org.rocksdb.RocksDB.loadLibrary
    val o =  (new org.rocksdb.Options).setCreateIfMissing(true)
    val io = (new org.rocksdb.IngestExternalFileOptions)
    val db = org.rocksdb.RocksDB.open(o, dbDirectory)

    println(files(sstDirectory))
    db.ingestExternalFile(files(sstDirectory), io)
  }

}
