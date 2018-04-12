package osmdiff.updater.schemas

import geotrellis.vectortile.Layer
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

  lazy val replacementFeatures: Seq[VTFeature] = {
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

  lazy val retainedFeatures: Seq[VTFeature] = {
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
}

object History extends SchemaBuilder {
  def apply(layer: Layer, features: Map[String, AugmentedDiffFeature]) =
    new History(layer, features)
}
