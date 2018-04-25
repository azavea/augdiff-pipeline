name := "indexer"

libraryDependencies ++= Seq(
  "com.amazonaws"     % "aws-java-sdk" % "1.7.4",
  "com.monovore"     %% "decline"      % "0.4.1",
  "org.apache.hadoop" % "hadoop-aws"   % "2.7.3",
  "org.apache.spark" %% "spark-core"   % "2.3.0" % "provided",
  "org.apache.spark" %% "spark-hive"   % "2.3.0" % "provided",
  "org.apache.spark" %% "spark-sql"    % "2.3.0" % "provided",
  "postgresql"        % "postgresql"   % "9.1-901-1.jdbc4"
)

assemblyJarName in assembly := "indexer.jar"

fork in Test := false
parallelExecution in Test := false
