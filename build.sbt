resolvers in ThisBuild ++= Seq("Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/", Resolver.mavenLocal)

name := "playGround"

version := "0.1-SNAPSHOT"

organization := "com.berlinsmartdata"

scalaVersion in ThisBuild := "2.11.8"

lazy val flinkVersion = "1.2.0"
lazy val awsVersion = "1.7.4" //  "1.11.8"; ===> is "1.11.118" requirement?
lazy val hadoopVersion = "2.7.2"


val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided"
  ,"org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided"
  ,"org.apache.flink" %% "flink-hadoop-compatibility" % flinkVersion

  ,"org.apache.flink" %% "flink-clients" % flinkVersion % "provided"
  ,"org.apache.flink" %% "flink-connector-kafka-0.10" % flinkVersion % "provided"
  ,"org.apache.flink" %% "flink-connector-kinesis" % flinkVersion % "provided"
  ,"org.apache.flink" %% "flink-connector-filesystem" % flinkVersion % "provided"

  // AWS deployment
  ,"com.amazonaws" % "aws-java-sdk" % awsVersion
  ,"com.amazonaws" % "aws-java-sdk-sts" % "1.11.111"

  // Hadoop - S3 dependencies
  ,"org.apache.hadoop" % "hadoop-aws" % hadoopVersion
  ,"org.apache.httpcomponents" % "httpcore" % "4.2.5"
  //,"org.apache.httpcomponents" % "httpclient" % "4.2.5"

)

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies
  )

mainClass in assembly := Some("com.berlinsmartdata.Job")

// make run command include the provided dependencies
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

// exclude Scala library from assembly
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
