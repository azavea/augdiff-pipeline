package osmdiff.updater.schemas

import geotrellis.vector.Feature
import geotrellis.vectortile._
import org.apache.log4j.Logger
import org.joda.time.DateTime
import osmdiff.updater.Implicits._
import osmdiff.updater._

class Urchn(override val layer: Layer, override val features: Map[String, AugmentedDiffFeature]) extends Schema {
  private lazy val logger = Logger.getLogger(getClass)

  private lazy val touchedFeatures: Map[String, Seq[VTFeature]] = {
    val featureIds = features.keySet

    layer
      .features
      .filter(f => featureIds.contains(f.data("__id")))
      .groupBy(f => f.data("__id"): String)
      .mapValues(fs => fs
        .sortWith(_.data("__minorVersion") < _.data("__minorVersion"))
        .sortWith(_.data("__version") < _.data("__version")))
  }

  private lazy val minorVersions: Map[String, Int] =
    features.values
      .groupBy(f => f.data.elementId)
      .mapValues(f => f.head.data)
      .mapValues(f => (f.elementId, f.version, f.timestamp))
      .mapValues { case (id, version, _) =>
        versionInfo.get(id) match {
          case Some((prevVersion, _, _)) if prevVersion < version => 0
          case Some((prevVersion, prevMinorVersion, _)) if prevVersion == version => prevMinorVersion + 1
          case _ => 0
        }
      }

  private lazy val authors: Map[String, Set[String]] = {
    touchedFeatures
      .mapValues(_.last)
      .mapValues(_.data("__authors").split(",").toSet)
  }

  private lazy val creation: Map[String, Long] =
    touchedFeatures
      .mapValues(_.head)
      .mapValues(_.data("__creation"))

  private lazy val versionInfo: Map[String, (Int, Int, DateTime)] =
    touchedFeatures
      .mapValues(_.last)
      .mapValues(f => (
        f.data("__version").toInt,
        f.data("__minorVersion").toInt,
        new DateTime(f.data("__updated"): Long)
      ))

  lazy val newFeatures: Seq[VTFeature] =
    features.values
      .filter(f =>
        versionInfo.get(f.data.elementId) match {
          case Some((_, _, prevTimestamp)) if f.data.timestamp.isAfter(prevTimestamp) => true
          case None => true
          case _ => false
        }
      )
      .map(x =>
        makeFeature(
          x,
          creation
            .getOrElse(x.data.elementId, x.data.timestamp.getMillis),
          authors
            .get(x.data.elementId)
            .map(_ + x.data.user)
            .getOrElse(Set(x.data.user)),
          minorVersions.get(x.data.elementId)))
      .filter(_.isDefined)
      .map(_.get)
      .toSeq

  private def makeFeature(feature: AugmentedDiffFeature, creation: Long, authors: Set[String], minorVersion: Option[Int]): Option[VTFeature] = {
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
            feature.geom, // when features are deleted, this will be the last geometry that was visible
            feature.data.tags.map {
              case (k, v) => (k, VString(v))
            } ++ Map(
              "__id" -> VString(elementId),
              "__changeset" -> VInt64(feature.data.changeset),
              "__updated" -> VInt64(feature.data.timestamp.getMillis),
              "__version" -> VInt64(feature.data.version),
              "__vtileGen" -> VInt64(System.currentTimeMillis),
              "__creation" -> VInt64(creation),
              "__authors" -> VString(authors.mkString(",")),
              "__lastAuthor" -> VString(feature.data.user)

            ) ++ minorVersion.map(v => Map("__minorVersion" -> VInt64(v))).getOrElse(Map.empty[String, Value])
          )
        )
      case _ => None
    }
  }
}

object Urchn extends SchemaBuilder {
  def apply(layer: Layer, features: Map[String, AugmentedDiffFeature]) =
    new Urchn(layer, features)
}
