name := "osmesa"

libraryDependencies ++= Seq(
  "org.locationtech.geotrellis" %% "geotrellis-vector" % "1.2.1" % "provided",
  "org.apache.spark"            %% "spark-core"        % "2.3.0" % "provided",
  "org.apache.spark"            %% "spark-hive"        % "2.3.0" % "provided",
  "org.apache.spark"            %% "spark-sql"         % "2.3.0" % "provided"
)

fork in Test := false
parallelExecution in Test := false
