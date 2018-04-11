package osmdiff

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths}

import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.vector.{Extent, Feature, Geometry, Line, MultiLine, MultiPoint, MultiPolygon, Point, Polygon}
import geotrellis.vectortile._
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer

package object updater {
  def exists(path: URI): Boolean = {
    // TODO S3 support
    new File(path).exists()
  }

  def read(path: URI): Array[Byte] = {
    // TODO S3 support
    Files.readAllBytes(Paths.get(path))
  }

  def write(path: URI, bytes: Array[Byte]) = {
    Files.write(Paths.get(path), bytes)
  }

  def makeFeature(feature: Feature[Geometry, AugmentedDiff], minorVersion: Option[Int] = None, validUntil: Option[Long] = None): Option[Feature[Geometry, Map[String, Value]]] = {
    val id = feature.data.id

    val elementId = feature.data.elementType match {
      case "node" => s"n$id"
      case "way" => s"w$id"
      case "relation" => s"r$id"
      case _ => id.toString
    }

    feature.mapGeom(_.reproject(LatLng, WebMercator)) match {
      case f if f.geom.isValid =>
        Some(
          Feature(
            f.geom,
            f.data.tags.map {
              case (k, v) => (k, VString(v))
            } ++ Map(
              "__id" -> VString(elementId),
              "__changeset" -> VInt64(f.data.changeset),
              "__updated" -> VInt64(f.data.timestamp.getMillis),
              "__validUntil" -> VInt64(validUntil.getOrElse(0L)),
              "__version" -> VInt64(f.data.version),
              "__uid" -> VInt64(f.data.uid),
              "__user" -> VString(f.data.user)
            ) ++ minorVersion.map(v => Map("__minorVersion" -> VInt64(v))).getOrElse(Map.empty[String, Value])
          )
        )
      case _ => None
    }
  }

  def updateFeature(feature: Feature[Geometry, Map[String, Value]], validUntil: DateTime): Feature[Geometry, Map[String, Value]] = {
    Feature(
      feature.geom,
      feature.data ++ Map("__validUntil" -> VInt64(validUntil.getMillis))
    )
  }

  def segregate(features: Seq[Feature[Geometry, Map[String, Value]]]): (Seq[Feature[Point, Map[String, Value]]],
    Seq[Feature[MultiPoint, Map[String, Value]]], Seq[Feature[Line, Map[String, Value]]], Seq[Feature[MultiLine,
    Map[String, Value]]], Seq[Feature[Polygon, Map[String, Value]]], Seq[Feature[MultiPolygon, Map[String,
    Value]]]) = {
    val points = ListBuffer[Feature[Point, Map[String, Value]]]()
    val multiPoints = ListBuffer[Feature[MultiPoint, Map[String, Value]]]()
    val lines = ListBuffer[Feature[Line, Map[String, Value]]]()
    val multiLines = ListBuffer[Feature[MultiLine, Map[String, Value]]]()
    val polygons = ListBuffer[Feature[Polygon, Map[String, Value]]]()
    val multiPolygons = ListBuffer[Feature[MultiPolygon, Map[String, Value]]]()

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

  def makeLayer(name: String, extent: Extent, features: Seq[Feature[Geometry, Map[String, Value]]]): Layer = {
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
