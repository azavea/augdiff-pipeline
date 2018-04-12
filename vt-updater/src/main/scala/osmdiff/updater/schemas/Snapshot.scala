package osmdiff.updater.schemas

import geotrellis.vectortile.Layer
import osmdiff.updater._

class Snapshot(override val layer: Layer, override val features: Map[String, AugmentedDiffFeature]) extends Schema {
  lazy val newFeatures: Seq[VTFeature] = {
    features.values
      .map(makeFeature(_))
      .filter(_.isDefined)
      .map(_.get)
      .toSeq
  }

  lazy val replacementFeatures: Seq[VTFeature] =
    Seq.empty[VTFeature]

  lazy val retainedFeatures: Seq[VTFeature] =
    Seq.empty[VTFeature]
}

object Snapshot extends SchemaBuilder {
  def apply(layer: Layer, features: Map[String, AugmentedDiffFeature]) =
    new Snapshot(layer, features)
}
