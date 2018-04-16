package osmdiff.updater

import geotrellis.vectortile.Layer

trait Schema {
  val layer: Layer
  val features: Map[String, AugmentedDiffFeature]

  val newFeatures: Seq[VTFeature]
  lazy val replacementFeatures: Seq[VTFeature] = Seq.empty[VTFeature]
  lazy val retainedFeatures: Seq[VTFeature] = Seq.empty[VTFeature]
}

trait SchemaBuilder {
  def apply(layer: Layer, features: Map[String, AugmentedDiffFeature]): Schema
}