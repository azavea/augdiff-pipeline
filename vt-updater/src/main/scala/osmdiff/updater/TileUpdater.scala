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
        logger.info(s"Fetching $sequence from $replicationSource and updating $tileSource from zoom $minZoom to $maxZoom")

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
              if (verbose) {
                println(filename)
              }

              val extent = sk.extent(layout)
              val tile = VectorTile.fromBytes(read(uri), extent)

              val feats = t.map(_._2)
              val featuresById = feats.groupBy(_.data.elementId)
              val featureIds = featuresById.keySet

              // load the target layer
              val layer = tile.layers(layerName)

              logger.debug(s"Inspecting ${layer.features.size.formatted("%,d")} features in layer '$layerName'")

              // fetch unmodified features
              val unmodifiedFeatures = layer
                .features
                .filterNot(f => featureIds.contains(f.data("__id").asInstanceOf[VString].value))

              val (replacementFeatures: Seq[Feature[Geometry, Map[String, Value]]], lastVersionsById: Map[String, (Int, Int, DateTime)]) = if (history) {
                val modifiedFeatures: Map[String, Seq[Feature[Geometry, Map[String, Value]]]] = layer
                  .features
                  .filter(f => featureIds.contains(f.data("__id").asInstanceOf[VString].value))
                  .groupBy(f => f.data("__id").asInstanceOf[VString].value)
                  .mapValues(fs => fs
                    .sortWith(_.data("__minorVersion").asInstanceOf[VInt64].value < _.data("__minorVersion").asInstanceOf[VInt64].value)
                    .sortWith(_.data("__version").asInstanceOf[VInt64].value < _.data("__version").asInstanceOf[VInt64].value))

                val featuresToKeep = modifiedFeatures
                  .mapValues(fs => fs.dropRight(1))
                  .values
                  .flatten
                  .toSeq

                logger.info(s"Keeping ${(unmodifiedFeatures.length + featuresToKeep.length).formatted("%,d")} features")

                val lastVersions = modifiedFeatures
                  .mapValues(_.last)

                val lastVersionsById = lastVersions
                  .mapValues(f => (
                    f.data("__version").asInstanceOf[VInt64].value.toInt,
                    f.data("__minorVersion").asInstanceOf[VInt64].value.toInt,
                    new DateTime(f.data("__updated").asInstanceOf[VInt64].value)
                  ))

                val replacedFeatures = lastVersions
                  .map { case (id, f) => updateFeature(f, featuresById(id).last.data.timestamp) }
                  .toSeq

                logger.info(s"Replacing ${replacedFeatures.length.formatted("%,d")} features")

                (featuresToKeep ++ replacedFeatures, lastVersionsById)
              } else {
                logger.info(s"Keeping ${unmodifiedFeatures.length.formatted("%,d")} features")
                (Seq.empty[Feature[Geometry, Map[String, Value]]], Map.empty[Long, (Int, Int)])
              }

              val newFeatures = if (history) {
                val minorVersions = feats
                  .groupBy(f => f.data.elementId)
                  .mapValues(f => f.head.data)
                  .mapValues(f => (f.elementId, f.version, f.timestamp))
                  .mapValues { case (id, version, timestamp) =>
                      lastVersionsById.get(id) match {
                        case Some((prevVersion, _, _)) if prevVersion < version => 0
                        case Some((prevVersion, prevMinorVersion, prevTimestamp)) if prevVersion == version && prevTimestamp.isBefore(timestamp) => prevMinorVersion + 1
                        case Some((prevVersion, prevMinorVersion, _)) if prevVersion == version => prevMinorVersion
                        case _ => 0
                      }
                  }

                feats.map(x => makeFeature(x, minorVersions.get(x.data.elementId))).filter(_.isDefined).map(_.get)
              } else {
                feats.map(makeFeature(_)).filter(_.isDefined).map(_.get)
              }

              logger.info(s"Adding ${newFeatures.length.formatted("%,d")} features")

              val updatedFeatures = unmodifiedFeatures ++ replacementFeatures ++ newFeatures

              val updatedLayer = makeLayer(layerName, extent, updatedFeatures)

              // merge all available layers into a new tile
              val newTile = VectorTile(tile.layers.updated(layerName, updatedLayer), extent)

              if (dryRun) {
                println(s"Would write ${newTile.toBytes.length.formatted("%,d")} bytes to $uri")
              } else {
                write(uri, newTile.toBytes)
              }
            }
          }
        }
    }
  }
)
