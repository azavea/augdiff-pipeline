package osmesa

object ProcessOSM {
  val NodeType: Byte = 1
  val WayType: Byte = 2
  val RelationType: Byte = 3
  val MultiPolygonRoles: Seq[String] = Set("", "outer", "inner").toSeq
}
