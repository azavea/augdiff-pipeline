package osmdiff

import java.net.URI
import java.nio.file.{Files, Paths}

import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling.{LayoutDefinition, ZoomedLayoutScheme}
import geotrellis.vector.{Extent, Feature, Geometry, Line, MultiLine, MultiPoint, MultiPolygon, Point, Polygon}
import geotrellis.vectortile._
import org.apache.log4j.Logger
import org.joda.time.DateTime
import osmdiff.updater.Implicits._

import scala.collection.mutable.ListBuffer

package object updater {
  private lazy val logger = Logger.getLogger(getClass)

  type AugmentedDiffFeature = Feature[Geometry, AugmentedDiff]
  type VTFeature = Feature[Geometry, VTProperties]
  type TypedVTFeature[T <: Geometry] = Feature[T, VTProperties]
  type VTProperties = Map[String, Value]

  val LayoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(WebMercator)

  def read(uri: URI): Option[Array[Byte]] = {
    // TODO S3 support

    val path = Paths.get(uri)

    if (Files.exists(path)) {
      Some(Files.readAllBytes(path))
    } else {
      None
    }
  }

  def write(path: URI, bytes: Array[Byte]) = {
    Files.write(Paths.get(path), bytes)
  }

  def makeFeature(feature: AugmentedDiffFeature, minorVersion: Option[Int] = None, validUntil: Option[Long] = None): Option[VTFeature] = {
    val id = feature.data.id

    val elementId = feature.data.elementType match {
      case "node" => s"n$id"
      case "way" => s"w$id"
      case "relation" => s"r$id"
      case _ => id.toString
    }

    feature match {
      case _ if feature.geom.isValid =>
        Some(
          Feature(
            feature.geom,
            feature.data.tags.map {
              case (k, v) => (k, VString(v))
            } ++ Map(
              "__id" -> VString(elementId),
              "__changeset" -> VInt64(feature.data.changeset),
              "__updated" -> VInt64(feature.data.timestamp.getMillis),
              "__validUntil" -> VInt64(validUntil.getOrElse(0L)),
              "__version" -> VInt64(feature.data.version),
              "__uid" -> VInt64(feature.data.uid),
              "__user" -> VString(feature.data.user)
            ) ++ minorVersion.map(v => Map("__minorVersion" -> VInt64(v))).getOrElse(Map.empty[String, Value])
          )
        )
      case _ => None
    }
  }

  def updateFeature(feature: VTFeature, validUntil: DateTime): VTFeature = {
    Feature(
      feature.geom,
      feature.data.updated("__validUntil", VInt64(validUntil.getMillis))
    )
  }

  def tile(features: Seq[AugmentedDiffFeature], layout: LayoutDefinition): Map[SpatialKey, Seq[AugmentedDiffFeature]] = {
    features
      .map(_.mapGeom(_.reproject(LatLng, WebMercator)))
      .filter(_.isValid)
      .flatMap { feat =>
        layout
          .mapTransform
          .keysForGeometry(feat.geom)
          .map { sk =>
            (sk, feat.mapGeom(_.intersection(sk.extent(layout)).toGeometry.orNull))
          }
          .filter(_._2.isValid)
      }
      .filter(x => Option(x._2).isDefined)
      .groupBy(_._1)
      .mapValues(_.map(_._2))
  }

  def updateTiles(tileSource: URI, layerName: String, zoom: Int, schemaType: SchemaBuilder, features: Seq[AugmentedDiffFeature], process: (SpatialKey, VectorTile) => Any): Unit = {
    val layout = LayoutScheme.levelForZoom(zoom).layout
    val tiles = tile(features, layout)

    for ((sk, feats) <- tiles) {
      val filename = s"$zoom/${sk.col}/${sk.row}.mvt"
      val uri = tileSource.resolve(filename)

      read(uri) match {
        case Some(bytes) =>
          val extent = sk.extent(layout)
          val tile = VectorTile.fromBytes(bytes, extent)

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

          val schema: Schema = schemaType(layer, featuresById)

          val retainedFeatures = schema.retainedFeatures
          val replacementFeatures = schema.replacementFeatures
          val newFeatures = schema.newFeatures

          if (newFeatures.nonEmpty) {
            logger.info(s"Adding ${newFeatures.length.formatted("%,d")} features")
          }

          unmodifiedFeatures ++ retainedFeatures ++ replacementFeatures ++ newFeatures match {
            case updatedFeatures if (replacementFeatures.length + newFeatures.length) > 0 =>
              val updatedLayer = makeLayer(layerName, extent, updatedFeatures)

              // merge all available layers into a new tile
              val newTile = VectorTile(tile.layers.updated(layerName, updatedLayer), extent)

              process(sk, newTile)
            case _ =>
              println(s"No changes to $uri; skipping")
          }
        case None =>
      }
    }
  }

  def segregate(features: Seq[VTFeature]): (Seq[TypedVTFeature[Point]], Seq[TypedVTFeature[MultiPoint]], Seq[TypedVTFeature[Line]], Seq[TypedVTFeature[MultiLine]], Seq[TypedVTFeature[Polygon]], Seq[TypedVTFeature[MultiPolygon]]) = {
    val points = ListBuffer[TypedVTFeature[Point]]()
    val multiPoints = ListBuffer[TypedVTFeature[MultiPoint]]()
    val lines = ListBuffer[TypedVTFeature[Line]]()
    val multiLines = ListBuffer[TypedVTFeature[MultiLine]]()
    val polygons = ListBuffer[TypedVTFeature[Polygon]]()
    val multiPolygons = ListBuffer[TypedVTFeature[MultiPolygon]]()

    features.foreach {
      case f@Feature(g: Point, _: Any) => points += f.mapGeom[Point](_.as[Point].get)
      case f@Feature(g: MultiPoint, _: Any) => multiPoints += f.mapGeom[MultiPoint](_.as[MultiPoint].get)
      case f@Feature(g: Line, _: Any) => lines += f.mapGeom[Line](_.as[Line].get)
      case f@Feature(g: MultiLine, _: Any) => multiLines += f.mapGeom[MultiLine](_.as[MultiLine].get)
      case f@Feature(g: Polygon, _: Any) => polygons += f.mapGeom[Polygon](_.as[Polygon].get)
      case f@Feature(g: MultiPolygon, _: Any) => multiPolygons += f.mapGeom[MultiPolygon](_.as[MultiPolygon].get)
    }

    (points, multiPoints, lines, multiLines, polygons, multiPolygons)
  }

  def makeLayer(name: String, extent: Extent, features: Seq[VTFeature]): Layer = {
    val (points, multiPoints, lines, multiLines, polygons, multiPolygons) = segregate(features)

    StrictLayer(
      name = name,
      tileWidth = 4096,
      version = 2,
      tileExtent = extent,
      points = points,
      multiPoints = multiPoints,
      lines = lines,
      multiLines = multiLines,
      polygons = polygons,
      multiPolygons = multiPolygons
    )
  }
}
