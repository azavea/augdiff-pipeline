name := "vt-updater"

libraryDependencies ++= Seq(
  "com.amazonaws"                % "aws-java-sdk"          % "1.7.4",
  "com.monovore"                %% "decline"               % "0.4.1",
  "org.locationtech.geotrellis" %% "geotrellis-vector"     % "1.2.1",
  "org.locationtech.geotrellis" %% "geotrellis-vectortile" % "1.2.1",
  "org.locationtech.geotrellis" %% "geotrellis-spark"      % "1.2.1"
)

assemblyJarName in assembly := "vt-updater.jar"

fork in Test := false
parallelExecution in Test := false
