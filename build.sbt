
name := "thumbnail-purge"

version := "0.1"

scalaVersion := "2.11.8"

assemblyMergeStrategy in assembly := {
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.2",
  "org.apache.spark" %% "spark-sql" % "2.3.2",
  "com.amazonaws" % "aws-java-sdk" % "1.7.4",
  "org.apache.hadoop" % "hadoop-aws" % "2.7.7",
  "com.databricks" %% "spark-avro" % "4.0.0"
)

resolvers += "SparkPackages" at "https://repos.spark-packages.org/"