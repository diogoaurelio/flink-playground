### Intro

A Flink application project using Scala and SBT.

To run and test your application use SBT invoke: 'sbt run'

In order to run your application from within IntelliJ, you have to select the classpath of the 'mainRunner' module in the run/debug configurations. Simply open 'Run -> Edit configurations...' and then select 'mainRunner' from the Use classpath of module dropbox.


### Flink setup for hadoop & S3 



#### Add Flink conf as environment variable

To run in intellij, set flink configuration directory as an environmental variable:

FLINK_CONF_DIR=/home/<user>/flink-config


### Common issues
 
1) Exception in thread "main" java.lang.NoClassDefFoundError: org/apache/flink/api/common/ExecutionConfig$GlobalJobParameters
This is probably due to the fact that you are running from intellij/terminal, and do not have all classes loaded into the class path. In other words, NOT all required Flink dependencies were implicitely loaded.
In order to run your application from within IntelliJ, you have to select the classpath of the 'mainRunner' module in the run/debug configurations. Simply open 'Run -> Edit configurations...' and then select 'mainRunner' from the Use classpath of module dropbox.

2) Caused by: java.io.IOException: No file system found with scheme s3, referenced in file URI 's3://YOUR-BUCKET-HERE/flink-basic-read-from-s3.txt'.
This is probably due to the fact that you have not specified your hadoop conf in environment variables. go ahead and add environment variable 
HADOOP_CONF_DIR=/PATH-TO-THIS-REPO/playground/src/main/resources/hadoop-config

# Notes: Lazy Evaluation
"
All Flink DataStream programs are executed lazily: When the program's main method is executed, the data loading and transformations do not happen directly. Rather, each operation is created and added to the program's plan. The operations are actually executed when the execution is explicitly triggered by an execute() call on the StreamExecutionEnvironment object. Whether the program is executed locally or on a cluster depends on the type of StreamExecutionEnvironment.
The lazy evaluation lets you construct sophisticated programs that Flink executes as one holistically planned unit.
"

# Notes: Data Sinks

"
Data Sinks


Data sinks consume DataStreams and forward them to files, sockets, external systems, or print them. Flink comes with a variety of built-in output formats that are encapsulated behind operations on the DataStreams:

writeAsText() / TextOuputFormat - Writes elements line-wise as Strings. The Strings are obtained by calling the toString() method of each element.

writeAsCsv(...) / CsvOutputFormat - Writes tuples as comma-separated value files. Row and field delimiters are configurable. The value for each field comes from the toString() method of the objects.

print() / printToErr() - Prints the toString() value of each element on the standard out / strandard error stream. Optionally, a prefix (msg) can be provided which is prepended to the output. This can help to distinguish between different calls to print. If the parallelism is greater than 1, the output will also be prepended with the identifier of the task which produced the output.

write() / FileOutputFormat - Method and base class for custom file outputs. Supports custom object-to-bytes conversion.

writeToSocket - Writes elements to a socket according to a SerializationSchema

addSink - Invokes a custom sink function. Flink comes bundled with connectors to other systems (such as Apache Kafka) that are implemented as sink functions.
"



## Custom Sinks - [HDFS Connector](https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/connectors/filesystem_sink.html)

In order to write partitioned Files to any file system - be it HDFS or S3 - Flink provides a specific connector, namely the HDFS connector.

To use it in sbt, add "flink-connector-filesystem" dependency in build.sbt:
```
val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided"
  ,"org.apache.flink" %% "flink-connector-filesystem" % flinkVersion % "provided"
```

The HDFS Connector uses the BucketingSink SinkFunction, as the following generic example shows:
```{scala}
val env = StreamExecutionEnvironment.createLocalEnvironment()

// Create a DataStream from a list of elements
val myInts = env.fromElements(1, 2, 3, 4, 5)

val input: DataStream[String] = env.fromCollection(data)
input.addSink(new BucketingSink[String]("/base/path"))
```

As stated in the code documentation: 
"
The {@code BucketingSink} can be writing to many buckets at a time, and it is responsible for managing
 * a set of active buckets. Whenever a new element arrives it will ask the {@code Bucketer} for the bucket
 * path the element should fall in.
"

It substitutes the previous and since Flink 1.2 deprecated class "RollingSink".