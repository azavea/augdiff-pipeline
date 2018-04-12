package osmdiff.updater

import java.io.File
import java.net.URI

import cats.implicits._
import com.monovore.decline._
import geotrellis.vector.io._
import org.apache.log4j.Logger
import osmdiff.updater.schemas._

import scala.io.Source

class TileUpdater

object TileUpdater extends CommandApp(
  name = "update-tiles",
  header = "Update vector tiles with changes from an augmented diff",
  main = {
    val rootURI = new File("").toURI

    val replicationSourceOpt = Opts.option[URI](
      "replication-source",
      short = "r",
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
    val schemaOpt = Opts.option[String](
      "schema",
      short = "s",
      metavar = "schema",
      help = "Schema"
    )
      .withDefault("snapshot")
      .validate("Must be a registered schema") {
        Schemas.keySet.contains(_)
      }
      .map {
        Schemas(_)
      }
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

    (replicationSourceOpt, tileSourceOpt, layerNameOpt, minZoomOpt, maxZoomOpt, schemaOpt, dryRunOpt, verboseOpt,
      sequenceOpt).mapN {
      (replicationSource, tileSource, layerName, minZoom, maxZoom, schema, dryRun, verbose, sequence) =>
        logger.info(s"Fetching $sequence from $replicationSource and updating $tileSource from zoom $minZoom to " +
          s"$maxZoom")

        val source = Source.fromFile(replicationSource.resolve(s"$sequence.json"))

        val features = source
          .getLines
          .map(_
            .drop(1) // remove the record separator at the beginning of a JSON record
            .parseGeoJson[AugmentedDiffFeature])
          .toSeq // this will be iterated over multiple times

        for (zoom <- minZoom to maxZoom) {
          updateTiles(tileSource, layerName, zoom, schema, features, (sk, tile) => {
            val filename = s"$zoom/${sk.col}/${sk.row}.mvt"
            val uri = tileSource.resolve(filename)

            if (dryRun) {
              println(s"Would write ${tile.toBytes.length.formatted("%,d")} bytes to $uri")
            } else {
              write(uri, tile.toBytes)
            }

            if (verbose) {
              println(filename)
            }
          })
        }
    }
  }
)
