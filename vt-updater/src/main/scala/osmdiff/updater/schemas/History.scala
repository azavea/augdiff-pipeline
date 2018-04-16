package osmdiff.updater.schemas

import geotrellis.vector.Feature
import geotrellis.vectortile._
import org.apache.log4j.Logger
import org.joda.time.DateTime
import osmdiff.updater.Implicits._
import osmdiff.updater._

class History(override val layer: Layer, override val features: Map[String, AugmentedDiffFeature]) extends Schema {
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

  private lazy val minorVersions: Map[String, Int] = {
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
  }

  private lazy val versionInfo: Map[String, (Int, Int, DateTime)] =
    touchedFeatures
      .mapValues(_.last)
      .mapValues(f => (
        f.data("__version").toInt,
        f.data("__minorVersion").toInt,
        new DateTime(f.data("__updated"): Long)
      ))

  lazy val newFeatures: Seq[VTFeature] = {
    features.values
      .filter(f =>
        versionInfo.get(f.data.elementId) match {
          case Some((_, _, prevTimestamp)) if f.data.timestamp.isAfter(prevTimestamp) => true
          case None => true
          case _ => false
        }
      )
      .map(x => makeFeature(x, minorVersions.get(x.data.elementId)))
      .filter(_.isDefined)
      .map(_.get)
      .toSeq
  }

  override lazy val replacementFeatures: Seq[VTFeature] = {
    val activeFeatures = touchedFeatures
      .filter {
        case (id, fs) =>
          features(id).data.timestamp.isAfter(fs.last.data("__updated"))
      }

    val featuresToReplace = activeFeatures
      .mapValues(fs => fs.filter(_.data("__validUntil").toLong == 0))
      .values
      .flatten
      .toSeq

    val replacedFeatures = featuresToReplace
      .map(f => updateFeature(f, features(f.data("__id")).data.timestamp))

    logger.info(s"Rewriting ${replacedFeatures.length.formatted("%,d")} features")

    replacedFeatures
  }

  override lazy val retainedFeatures: Seq[VTFeature] = {
    val activeFeatures = touchedFeatures
      .filter {
        case (id, fs) =>
          features(id).data.timestamp.isAfter(fs.last.data("__updated"))
      }

    activeFeatures
      .mapValues(fs => fs.filterNot(_.data("__validUntil").toLong == 0))
      .values
      .flatten
      .toSeq
  }

  private def makeFeature(feature: AugmentedDiffFeature, minorVersion: Option[Int], validUntil: Option[Long] = None): Option[VTFeature] = {
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
              "__validUntil" -> VInt64(validUntil.getOrElse(0L)),
              "__version" -> VInt64(feature.data.version),
              "__uid" -> VInt64(feature.data.uid),
              "__user" -> VString(feature.data.user),
              "__visible" -> VBool(feature.data.visible)
            ) ++ minorVersion.map(v => Map("__minorVersion" -> VInt64(v))).getOrElse(Map.empty[String, Value])
          )
        )
      case _ => None
    }
  }

  private def updateFeature(feature: VTFeature, validUntil: DateTime): VTFeature = {
    Feature(
      feature.geom,
      feature.data.updated("__validUntil", VInt64(validUntil.getMillis))
    )
  }
}

object History extends SchemaBuilder {
  def apply(layer: Layer, features: Map[String, AugmentedDiffFeature]) =
    new History(layer, features)
}
