lazy val commonSettings = Seq(
  organization := "com.azavea",
  version := "0.1",
  cancelable in Global := true,
  scalaVersion in ThisBuild := "2.11.12",
  scalacOptions := Seq(
    "-deprecation",
    "-unchecked",
    "-feature",
    "-language:implicitConversions",
    "-language:reflectiveCalls",
    "-language:higherKinds",
    "-language:postfixOps",
    "-language:existentials",
    "-language:experimental.macros",
    "-feature",
    "-Ypartial-unification",
    "-Ypatmat-exhaust-depth", "100"
  ),
  resolvers ++= Seq(),
  shellPrompt := { s => Project.extract(s).currentProject.id + " > " }
)

lazy val root = Project("osm-diff", file("."))
  .settings(commonSettings: _*)

lazy val ad =
  project
    .settings(commonSettings: _*)
    .dependsOn(root)
