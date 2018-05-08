name := "ad"

libraryDependencies ++= Seq(
  "com.amazonaws"            % "aws-java-sdk"  % "1.7.4",
  "com.monovore"             %% "decline"       % "0.4.1",
  "io.circe"                 %% "circe-core"    % "0.9.3",
  "io.circe"                 %% "circe-generic" % "0.9.3",
  "io.circe"                 %% "circe-parser"  % "0.9.3",
  "org.apache.hadoop"         % "hadoop-aws"    % "2.7.3",
  "org.apache.spark"         %% "spark-core"    % "2.3.0" % "provided",
  "org.apache.spark"         %% "spark-hive"    % "2.3.0" % "provided",
  "org.apache.spark"         %% "spark-sql"     % "2.3.0" % "provided",
  "org.locationtech.geotrellis" %% "geotrellis-vector" % "1.2.1",
  "org.openstreetmap.osmosis" % "osmosis-core"  % "0.46",
  "org.openstreetmap.osmosis" % "osmosis-xml"   % "0.46",
  "org.postgresql"            % "postgresql"    % "9.4.1212"
)

assemblyJarName in assembly := "ad.jar"

fork in Test := false
parallelExecution in Test := false
