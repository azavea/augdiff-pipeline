package osmdiff.updater

import java.io.File
import java.net.URI

import cats.implicits._
import com.monovore.decline._
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.spark.tiling.ZoomedLayoutScheme
import geotrellis.vector.io._
import geotrellis.vector.{Feature, Geometry}
import geotrellis.vectortile._
import org.apache.log4j.Logger
import org.joda.time.DateTime
import osmdiff.updater.Implicits._

import scala.io.Source

class TileUpdater

object TileUpdater extends CommandApp(
  name = "update-tiles",
  header = "Update vector tiles with changes from an augmented diff",
  main = {
    val rootURI = new File("").toURI

    val replicationSourceOpt = Opts.option[URI](
      "replication-source",
      short = "s",
      metavar = "uri",
      help = "URI prefix for replication files"
    ).withDefault(rootURI)
    val tileSourceOpt = Opts.option[URI](
      "tile-source",
      short = "t",
      metavar = "uri",
      help = "URI prefix for vector tiles to update"
    ).withDefault(rootURI)
    val layerNameOpt = Opts.option[String](
      "layer-name",
      short = "l",
      metavar = "layer name",
      help = "Layer to modify"
    )
    val minZoomOpt = Opts.option[Int](
      "min-zoom",
      short = "z",
      metavar = "zoom",
      help = "Minimum zoom to consider"
    )
    val maxZoomOpt = Opts.option[Int](
      "max-zoom",
      short = "Z",
      metavar = "zoom",
      help = "Maximum zoom to consider"
    )
    val historyOpt = Opts.flag(
      "history",
      short = "H",
      help = "Treat vector tile source as history (append new features, etc)"
    ).orFalse
    val dryRunOpt = Opts.flag(
      "dry-run",
      short = "n",
      help = "Dry run"
    ).orFalse
    val verboseOpt = Opts.flag(
      "verbose",
      short = "v",
      help = "Be verbose"
    ).orFalse
    val sequenceOpt = Opts.argument[Int]("sequence")

    val logger = Logger.getLogger(classOf[TileUpdater])

    (replicationSourceOpt, tileSourceOpt, layerNameOpt, minZoomOpt, maxZoomOpt, historyOpt, dryRunOpt, verboseOpt,
      sequenceOpt).mapN {
      (replicationSource, tileSource, layerName, minZoom, maxZoom, history, dryRun, verbose, sequence) =>
        logger.info(s"Fetching $sequence from $replicationSource and updating $tileSource from zoom $minZoom to " +
          s"$maxZoom")

        val source = Source.fromFile(replicationSource.resolve(s"$sequence.json"))

        val features = source
          .getLines
          .map(_.drop(1).parseGeoJson[Feature[Geometry, AugmentedDiff]])
          .toSeq // this will be iterated over multiple times

        val layoutScheme = ZoomedLayoutScheme(WebMercator)


        for (zoom <- minZoom to maxZoom) {
          val layout = layoutScheme.levelForZoom(zoom).layout

          // get spatial keys covered by available features
          val keys = features.flatMap { feat =>
            val g = feat.geom.reproject(LatLng, WebMercator)
            val keys = layout.mapTransform.keysForGeometry(g)
            keys.map { k => (k, feat) }
          }

          // group into a Map
          val tiles = keys.groupBy(_._1)

          for ((sk, t) <- tiles) {
            val filename = s"$zoom/${sk.col}/${sk.row}.mvt"
            val uri = tileSource.resolve(filename)

            if (exists(uri)) {
              val extent = sk.extent(layout)
              val tile = VectorTile.fromBytes(read(uri), extent)

              // reproject and clip geometries to the target extent
              val feats = t
                .map(_._2)
                .map(_.mapGeom(_.reproject(LatLng, WebMercator)))
                .filter(_.geom.isValid)
                .map(_.mapGeom(_.intersection(extent).toGeometry.orNull))
                .filter(f => Option(f.geom).isDefined)

              val featuresById = feats
                .groupBy(_.data.elementId)
                .mapValues(fs => fs.head)
              val featureIds = featuresById.keySet

              // load the target layer
              val layer = tile.layers(layerName)

              logger.debug(s"Inspecting ${layer.features.size.formatted("%,d")} features in layer '$layerName'")

              // fetch unmodified features
              val unmodifiedFeatures = layer
                .features
                .filterNot(f => featureIds.contains(f.data("__id")))

              val (retainedFeatures, replacementFeatures, lastVersionsById: Map[String, (Int, Int, DateTime)]) = if (history) {
                val modifiedFeatures: Map[String, Seq[Feature[Geometry, Map[String, Value]]]] = layer
                  .features
                  .filter(f => featureIds.contains(f.data("__id")))
                  .groupBy(f => f.data("__id"): String)
                  .mapValues(fs => fs
                    .sortWith(_.data("__minorVersion") < _.data("__minorVersion"))
                    .sortWith(_.data("__version") < _.data("__version")))

                val activeFeatures = modifiedFeatures
                  .filter {
                    case (id, fs) =>
                      featuresById(id).data.timestamp.isAfter(fs.last.data("__updated"))
                  }

                val featuresToKeep = activeFeatures
                  .mapValues(fs => fs.filterNot(_.data("__validUntil").toLong == 0))
                  .values
                  .flatten
                  .toSeq

                val featuresToReplace = activeFeatures
                  .mapValues(fs => fs.filter(_.data("__validUntil").toLong == 0))
                  .values
                  .flatten
                  .toSeq

                val lastVersions = modifiedFeatures
                  .mapValues(_.last)

                val lastVersionsById = lastVersions
                  .mapValues(f => (
                    f.data("__version").toInt,
                    f.data("__minorVersion").toInt,
                    new DateTime(f.data("__updated"): Long)
                  ))

                val replacedFeatures = featuresToReplace
                  .map(f => updateFeature(f, featuresById(f.data("__id")).data.timestamp))

                logger.info(s"Rewriting ${replacedFeatures.length.formatted("%,d")} features")

                (featuresToKeep, replacedFeatures, lastVersionsById)
              } else {
                logger.info(s"Keeping ${unmodifiedFeatures.length.formatted("%,d")} features")
                (Seq.empty[Feature[Geometry, Map[String, Value]]], Seq.empty[Feature[Geometry, Map[String, Value]]], Map.empty[Long, (Int, Int)])
              }

              val newFeatures = if (history) {
                val minorVersions = feats
                  .groupBy(f => f.data.elementId)
                  .mapValues(f => f.head.data)
                  .mapValues(f => (f.elementId, f.version, f.timestamp))
                  .mapValues { case (id, version, _) =>
                    lastVersionsById.get(id) match {
                      case Some((prevVersion, _, _)) if prevVersion < version => 0
                      case Some((prevVersion, prevMinorVersion, _)) if prevVersion == version => prevMinorVersion + 1
                      case _ => 0
                    }
                  }

                feats
                  .filter(f =>
                    lastVersionsById.get(f.data.elementId) match {
                      case Some((_, _, prevTimestamp)) if f.data.timestamp.isAfter(prevTimestamp) => true
                      case None => true
                      case _ => false
                    }
                  )
                  .map(x => makeFeature(x, minorVersions.get(x.data.elementId)))
                  .filter(_.isDefined)
                  .map(_.get)
              } else {
                feats
                  .map(makeFeature(_))
                  .filter(_.isDefined)
                  .map(_.get)
              }

              if (newFeatures.nonEmpty) {
                logger.info(s"Adding ${newFeatures.length.formatted("%,d")} features")
              }

              unmodifiedFeatures ++ retainedFeatures ++ replacementFeatures ++ newFeatures match {
                case updatedFeatures if (replacementFeatures.length + newFeatures.length) > 0 =>
                  val updatedLayer = makeLayer(layerName, extent, updatedFeatures)

                  // merge all available layers into a new tile
                  val newTile = VectorTile(tile.layers.updated(layerName, updatedLayer), extent)

                  if (dryRun) {
                    println(s"Would write ${newTile.toBytes.length.formatted("%,d")} bytes to $uri")
                  } else {
                    write(uri, newTile.toBytes)
                  }

                  if (verbose) {
                    println(filename)
                  }
                case _ =>
                  println(s"No changes to $uri; skipping")
              }
            }
          }
        }
    }
  }
)
