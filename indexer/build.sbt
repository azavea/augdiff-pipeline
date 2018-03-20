name := "indexer"

libraryDependencies ++= Seq(
  "com.amazonaws"     % "aws-java-sdk" % "1.7.4",
  "org.apache.hadoop" % "hadoop-aws"   % "2.7.3",
  "org.apache.spark" %% "spark-core"   % "2.3.0" % "provided",
  "org.apache.spark" %% "spark-hive"   % "2.3.0" % "provided",
  "org.apache.spark" %% "spark-sql"    % "2.3.0" % "provided"
)

// libraryDependencies in console ++= Seq(
//   "org.apache.spark" %% "spark-core" % "2.3.0",
//   "org.apache.spark" %% "spark-hive" % "2.3.0",
//   "org.apache.spark" %% "spark-sql"  % "2.3.0"
// )

// initialCommands in console :=
//   """
// import osmdiff.indexer._
// val spark = Indexer.sparkSession()
//   """

assemblyJarName in assembly := "indexer.jar"

fork in Test := false
parallelExecution in Test := false
