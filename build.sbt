lazy val commonSettings = Seq(
  organization := "osmdiff",
  version := "0",
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
  resolvers ++= Seq(
    "jcraft" at "https://mvnrepository.com/artifact/com.jcraft/jsch",
    "jets3t" at "https://mvnrepository.com/artifact/net.java.dev.jets3t/jets3t"
  ),
  assemblyMergeStrategy in assembly := {
    case s if s.startsWith("META-INF/services") => MergeStrategy.concat
    case "reference.conf" | "application.conf"  => MergeStrategy.concat
    case "META-INF/MANIFEST.MF" | "META-INF\\MANIFEST.MF" => MergeStrategy.discard
    case "META-INF/ECLIPSEF.RSA" | "META-INF/ECLIPSEF.SF" => MergeStrategy.discard
    case _ => MergeStrategy.first
  },
  shellPrompt := { s => Project.extract(s).currentProject.id + " > " }
)

lazy val root = (project in file("."))
  .aggregate(common, indexer, ad)
  .settings(commonSettings: _*)

lazy val common = (project in file("common"))
  .settings(commonSettings: _*)

lazy val indexer = (project in file ("indexer"))
  .dependsOn(common)
  .settings(commonSettings: _*)

lazy val ad = (project in file ("ad"))
  .dependsOn(common)
  .settings(commonSettings: _*)

lazy val tileUpdater = (project in file("vt-updater"))
  .dependsOn(common)
  .settings(commonSettings: _*)
