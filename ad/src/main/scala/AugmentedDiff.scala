package osmdiff

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import org.apache.commons.io.FileUtils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import org.openstreetmap.osmosis.xml.common.CompressionMethod
import org.openstreetmap.osmosis.xml.v0_6.XmlChangeReader

import scala.collection.mutable

import java.io._
import java.net.{URI, URL}

import cats.implicits._
import com.monovore.decline._


object AugmentedDiff {

  val logger = {
    val logger = Logger.getLogger(this.getClass)
    logger.setLevel(Level.INFO)
    logger
  }

  // state
  val paths = mutable.ArrayBuffer.empty[Path]
  val rows_from_memory = mutable.ArrayBuffer.empty[Row]

  // Given a set of update rows (`rows_from_update`), a set of rows
  // from memory that haven't been flushed to storage yet
  // (`rows_from_memory`), and a set of dependency arrows (`edges`
  // [edge.a is an entity, edge.b is a dependency of that entity]),
  // compute the complete set of rows needed to render the update.
  //
  // The set `edges` is passed-in because it has already been computed
  // as part of the index-updating process.
  def augment(
    conf: Configuration,
    from_update: Array[Row],
    from_memory: Array[Row],
    edges: Set[ComputeIndexLocal.Edge],
    externalLocation: String
  ): Array[Row] = {

    // (partition, id, type) triples from the update rows.
    //
    // This may look strange because the update rows are already in
    // hand, but these do need to be loaded from storage in the case
    // where something has been *modified* rather than just added.
    val triples_from_updates =
      from_update.map({ row =>
        val id = row.getLong(1)
        val tipe = row.getString(2)
        val p = Common.partitionNumberFn(id, tipe)
        (p, id, tipe)
      }).toSet

    // (partition, id, type) triples form the dependency rows
    val triples_from_deps =
      edges
        .flatMap({ edge =>
          val aId = Common.longToIdFn(edge.a)
          val aType = Common.longToTypeFn(edge.a)
          val ap = Common.partitionNumberFn(aId, aType)
          val bId = Common.longToIdFn(edge.b)
          val bType = Common.longToTypeFn(edge.b)
          val bp = Common.partitionNumberFn(bId, bType)
          List((ap, aId, aType), (bp, bId, bType))
        }).toSet

    val triples = triples_from_updates ++ triples_from_deps
    val keyedTriples = triples.groupBy(_._1) // mapping from partition to list of triples
    val pairs: Set[(Long, String)] = keyedTriples.values.flatMap({ s => s.map({ t => (t._2, t._3) }) }).toSet
    val from_memory2 = from_memory.filter({ row => // filter out uninteresting in-memory rows
      val id = row.getLong(1)
      val tipe = row.getString(2)
      val pair = (id, tipe)
      pairs.contains(pair)
    })
    val from_storage = OrcBackend.load(conf, paths.toArray, keyedTriples, pairs)

    (from_update ++ from_memory2 ++ from_storage).distinct
  }

  def osc2json(
    conf: Configuration,
    oscfile: String, jsonfile: String,
    uri: String, props: java.util.Properties,
    externalLocation: String
  ): Unit = {
    logger.info(s"$oscfile -> $jsonfile")

    var i: Int = 1; while (i <= (1<<8)) {
      // File
      val file: File =
        try {
          if (oscfile.startsWith("hdfs:") || oscfile.startsWith("file:") || oscfile.startsWith("s3a:")) {
            val path = new Path(oscfile)
            val uri = new URI(oscfile)
            val fs = FileSystem.get(uri, conf)
            val tmp = File.createTempFile("abcdefg", ".osc")
            tmp.deleteOnExit // XXX probably not sufficient for long-running process
            fs.copyToLocalFile(path, new Path(tmp.getAbsolutePath))
            tmp
          }
          else if (oscfile.startsWith("http:") || oscfile.startsWith("https:")) {
            val tmp = File.createTempFile("abcdefg", ".osc")
            val url = new URL(oscfile)
            tmp.deleteOnExit // XXX probably not sufficient for long-running process
            FileUtils.copyURLToFile(url, tmp)
            tmp
          }
          else {
            val tmp = new File(oscfile)
            tmp.setWritable(false)
            if (!tmp.exists || tmp.isDirectory) throw new java.io.IOException
            tmp
          }
        }
        catch {
          case e @ (_ : java.net.ConnectException | _ : java.io.IOException | _ : java.io.FileNotFoundException | _ : com.amazonaws.AmazonClientException) =>
            i=i*2
            logger.info(s"Problem opening $oscfile, sleeping for $i seconds then trying again...")
            Thread.sleep(i * 1000)
            null
        }

      // Change Reader, Change Augmenter
      if (file != null) {
        val cr =
          if (oscfile.endsWith(".osc.bz2")) new XmlChangeReader(file, true, CompressionMethod.BZip2)
          else if (oscfile.endsWith(".osc.gz")) new XmlChangeReader(file, true, CompressionMethod.GZip)
          else new XmlChangeReader(file, true, CompressionMethod.None)
        val ca = new ChangeAugmenter(conf, rows_from_memory, uri, props, jsonfile, externalLocation)

        cr.setChangeSink(ca)
        try {
          cr.run
          i = Int.MaxValue
        }
        catch {
          case e: org.openstreetmap.osmosis.core.OsmosisRuntimeException =>
            i=i*2
            logger.info(s"Problem parsing $oscfile, sleeping for $i seconds then trying again...")
            Thread.sleep(i*1000)
        }
      }
    }
  }

  def numberToStr(n: Int): String =
    (n.toString.reverse ++ "000").take(3).reverse

}

object AugmentedDiffApp extends CommandApp(
  name = "Augmented Differ",
  header = "Augment diffs",
  main = {
    val spark = Common.sparkSession("Augmented Diff")
    import spark.implicits._

    Common.denoise

    val osctemplate =
      Opts.option[String]("osctemplate", help = "OSC input template")
    val jsontemplate =
      Opts.option[String]("jsontemplate", help = "JSON output template")
    val range =
      Opts.option[String]("range", help = "The range of OSC files to consume")
    val postgresHost =
      Opts.option[String]("postgresHost", help = "PostgreSQL host").withDefault("localhost")
    val postgresPort =
      Opts.option[Int]("postgresPort", help = "PostgreSQL port").withDefault(5432)
    val postgresUser =
      Opts.option[String]("postgresUser", help = "PostgreSQL username").withDefault("hive")
    val postgresPassword =
      Opts.option[String]("postgresPassword", help = "PostgreSQL password").withDefault("hive")
    val postgresDb =
      Opts.option[String]("postgresDb", help = "PostgreSQL database").withDefault("osm")
    val external =
      Opts.option[String]("external", help = "External location of OSM table")

    (osctemplate, jsontemplate, range, postgresHost, postgresPort, postgresUser, postgresPassword, postgresDb, external).mapN({
      (osctemplate, jsontemplate, range, postgresHost, postgresPort, postgresUser, postgresPassword, postgresDb, external) =>

      val uri = s"jdbc:postgresql://${postgresHost}:${postgresPort}/${postgresDb}"
      val props = {
        val ps = new java.util.Properties()
        ps.put("user", postgresUser)
        ps.put("password", postgresPassword)
        ps.put("driver", "org.postgresql.Driver")
        ps
      }

      val stream = range.split(",") match {
        case Array(start, "-1") => Stream.from(start.toInt)
        case Array(start, end) => Stream.from(start.toInt).take(end.toInt - start.toInt + 1)
        case _ => throw new Exception("Oh no")
      }

      val conf = spark.sparkContext.hadoopConfiguration
      OrcBackend.listFiles(conf, AugmentedDiff.paths, external)

      var counter = 0
      val saveInterval = 5
      stream.foreach({ i =>
        val ccc = AugmentedDiff.numberToStr(i % 1000)
        val bbb = AugmentedDiff.numberToStr((i / 1000) % 1000)
        val aaa = AugmentedDiff.numberToStr((i / 1000000) % 1000)
        val jsonfile = jsontemplate.replace("AAA", aaa).replace("BBB", bbb).replace("CCC", ccc)
        val oscfile = osctemplate.replace("AAA", aaa).replace("BBB", bbb).replace("CCC", ccc)
        AugmentedDiff.osc2json(conf, oscfile, jsonfile, uri, props, external)

        counter=counter+1
        if ((counter % saveInterval) == 0) { // periodic save
          val df = spark.createDataFrame(
            spark.sparkContext.parallelize(AugmentedDiff.rows_from_memory, 1),
            StructType(Common.osmSchema))
          OrcBackend.save(df, "osm", external, "append")
          OrcBackend.listFiles(conf, AugmentedDiff.paths, external)
          AugmentedDiff.rows_from_memory.clear
        }
      })
    })
  }
)
