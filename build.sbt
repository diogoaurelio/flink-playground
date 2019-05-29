ThisBuild / resolvers ++= Seq(
    "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
    Resolver.mavenLocal
)

name := "playground"

version := "0.1-SNAPSHOT"

organization := "com.datenn"

ThisBuild / scalaVersion := "2.11.12"

val flinkVersion = "1.8.0"
val awsVersion = "1.7.4" // "1.11.8" //
val hadoopVersion = "2.7.2"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided"
  ,"org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided"
  ,"org.apache.flink" %% "flink-clients" % flinkVersion % "provided"
  ,"org.apache.flink" %% "flink-connector-kafka-0.10" % flinkVersion % "provided"
  //,"org.apache.flink" %% "flink-connector-kinesis" % flinkVersion % "provided"
  ,"org.apache.flink" %% "flink-connector-filesystem" % flinkVersion % "provided"

  // AWS deployment
  ,"com.amazonaws" % "aws-java-sdk" % awsVersion
  ,"com.amazonaws" % "aws-java-sdk-sts" % "1.11.111"

  // Hadoop dependencies
  ,"org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion
  //,"org.apache.hadoop" % "hadoop-mapreduce" % hadoopVersion
  
  // Hadoop - S3 dependencies
  ,"org.apache.hadoop" % "hadoop-aws" % hadoopVersion


,"org.apache.httpcomponents" % "httpcore" % "4.2.5"
  //,"org.apache.httpcomponents" % "httpclient" % "4.2.5"

  // Hadoop - Storage formats
  ,"org.apache.avro" % "avro" % "1.7.7"
  //,"org.apache.avro" % "avro-mapred" % "1.7.7"
  //,"org.apache.parquet" % "parquet-avro" % "1.8.1"


  // testing
  ,"org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies
  )

assembly / mainClass := Some("com.datenn.s3.BasicS3ReadWrite")

// make run command include the provided dependencies
Compile / run  := Defaults.runTask(Compile / fullClasspath,
                                   Compile / run / mainClass,
                                   Compile / run / runner
                                  ).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

// exclude Scala library from assembly
assembly / assemblyOption  := (assembly / assemblyOption).value.copy(includeScala = false)
