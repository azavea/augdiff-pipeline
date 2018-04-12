package osmdiff.updater

import geotrellis.vectortile.Layer

trait Schema {
  val layer: Layer
  val features: Map[String, AugmentedDiffFeature]

  val newFeatures: Seq[VTFeature]
  val retainedFeatures: Seq[VTFeature]
  val replacementFeatures: Seq[VTFeature]
}

trait SchemaBuilder {
  def apply(layer: Layer, features: Map[String, AugmentedDiffFeature]): Schema
}