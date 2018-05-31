package osmdiff.updater

package object schemas {
  val Schemas: Map[String, SchemaBuilder] = Map(
    "history" -> History,
    "snapshot" -> Snapshot,
    "urchn" -> Urchn
  )
}
