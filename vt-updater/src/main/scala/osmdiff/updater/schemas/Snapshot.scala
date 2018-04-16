package osmdiff.updater.schemas

import geotrellis.vector.Feature
import geotrellis.vectortile.{Layer, VInt64, VString}
import osmdiff.updater._

class Snapshot(override val layer: Layer, override val features: Map[String, AugmentedDiffFeature]) extends Schema {
  lazy val newFeatures: Seq[VTFeature] = {
    features.values
      .filter(_.data.visible)
      .map(makeFeature)
      .filter(_.isDefined)
      .map(_.get)
      .toSeq
  }

  lazy val replacementFeatures: Seq[VTFeature] =
    Seq.empty[VTFeature]

  lazy val retainedFeatures: Seq[VTFeature] =
    Seq.empty[VTFeature]

  private def makeFeature(feature: AugmentedDiffFeature): Option[VTFeature] = {
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
              "__version" -> VInt64(feature.data.version),
              "__uid" -> VInt64(feature.data.uid),
              "__user" -> VString(feature.data.user)
            )
          )
        )
      case _ => None
    }
  }
}

object Snapshot extends SchemaBuilder {
  def apply(layer: Layer, features: Map[String, AugmentedDiffFeature]) =
    new Snapshot(layer, features)
}
