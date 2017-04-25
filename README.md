### Intro

A Flink application project using Scala and SBT.

To run and test your application use SBT invoke: 'sbt run'

In order to run your application from within IntelliJ, you have to select the classpath of the 'mainRunner' module in the run/debug configurations. Simply open 'Run -> Edit configurations...' and then select 'mainRunner' from the Use classpath of module dropbox.


### Flink setup for hadoop & S3 


#### Add Flink conf as environment variable

To run in intellij, set flink configuration directory as an environmental variable:

FLINK_CONF_DIR=/home/<user>/flink-config


### Common issues running examples
 
1) java.lang.NoClassDefFoundError

```
Exception (...) Caused by: java.lang.NoClassDefFoundError: org/apache/flink/api/common/ExecutionConfig$GlobalJobParameters
```

This is probably due to the fact that you are running from intellij/terminal, and do not have all classes loaded into the class path. In other words, NOT all required Flink dependencies were implicitely loaded.
In order to run your application from within IntelliJ, you have to select the classpath of the 'mainRunner' module in the run/debug configurations. Simply open 'Run -> Edit configurations...' and then select 'mainRunner' from the Use classpath of module dropbox.

2) java.lang.ClassNotFoundException
```
Exception (...) Caused by: java.lang.ClassNotFoundException: org.apache.flink.api.common.typeinfo.TypeInformation
```
Same as point 1.

3) could not find implicit value for evidence parameter of type org.apache.flink.api.common.typeinfo.TypeInformation[String]

This will prompt as a compilation error, even before you run your Application, such as, for example:
```
"Error:(15, 30) could not find implicit value for evidence parameter of type org.apache.flink.api.common.typeinfo.TypeInformation[String]
  val text = env.fromElements("To be, or not to be,--that is the question:--",
```

Make sure you have this import statement on the top of your Flink Application:

```{scala}
import org.apache.flink.streaming.api.scala._
```

4) 
```
Exception (...) Caused by: java.io.IOException: No file system found with scheme s3, referenced in file URI 's3://YOUR-BUCKET-HERE/flink-basic-read-from-s3.txt'.
```
This is probably due to the fact that you have not specified your hadoop conf in environment variables. go ahead and add environment variable 
HADOOP_CONF_DIR=/PATH-TO-THIS-REPO/playground/src/main/resources/hadoop-config



# Flink Basics

Aggregations on Datastreams are different from aggregations on Datasets, as they are meant to be infinite. 
Thus, it logically follows that one cannot, for example, count all elements in a Datastream.

Thus windowed aggregations play a specially relevant role in Datastreams - as a means of setting bounds.


## Batch VS Streaming

Batch special case of streaming.

How does the program recognize if it is running a batch or streaming context/environment? Easy, you define which type of execution context/environment you want. 

For batch environment, one would do as follows:
```{scala}
import org.apache.flink.api.scala.ExecutionEnvironment

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
```

Note: If you're familiar with spark, then this exactly equivalent to SparkContext(). 


So, the program will implicitely know that it is in a batch environment, and further work with DataSet[T] API. In other words, when using ExecutionEnvironment, a DataSet[T] is automatically returned:

```{scala}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.DataSet

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
val text: DataSet[String] = env1.fromElements("To be, or not to be,--that is the question:--",
      "Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune",
      "Or to take arms against a sea of troubles,")
```

For streaming environment:
```{scala}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
```

Streaming environment work with DataStream[T], thus by setting env to StreamingExecutionEnvironment, the program automatically returns DataStream[T]:
```{scala}
 import org.apache.flink.api.scala.ExecutionEnvironment
 import org.apache.flink.api.scala.DataSet
 
 val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
 val text: DataStream[String] = env1.fromElements("To be, or not to be,--that is the question:--",
       "Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune",
       "Or to take arms against a sea of troubles,")
```


## Lazy Evaluation
"
All Flink DataStream programs are executed lazily: When the program's main method is executed, the data loading and transformations do not happen directly. Rather, each operation is created and added to the program's plan. The operations are actually executed when the execution is explicitly triggered by an execute() call on the StreamExecutionEnvironment object. Whether the program is executed locally or on a cluster depends on the type of StreamExecutionEnvironment.
The lazy evaluation lets you construct sophisticated programs that Flink executes as one holistically planned unit.
"

Execution is only triggered in final line:

```{scala}
env.execute("My Flink application")
```

## Operations

### Data partitioning - Windows

First basic distinction is two main types of windows: keyed and non-keyed windows.

.window()
operates on already partioned data, in this case keyed data. data Windows can be defined on already partitioned KeyedStreams.

As provided in [Flink Blog post introducing stream windows](https://flink.apache.org/news/2015/12/04/Introducing-windows.html):
```{scala}
val input: DataStream[IN] = ...

// created a keyed stream using a key selector function
val keyed: KeyedStream[IN, KEY] = input
  .keyBy(myKeySel: (IN) => KEY)
  
// create windowed stream using a WindowAssigner
var windowed: WindowedStream[IN, KEY, WINDOW] = keyed
  .window(myAssigner: WindowAssigner[IN, WINDOW])
```

Exception is .windowAll() which groups data according to some characteristic, such as data arrived in last X amount of time.
However, beware in terms of performance: it is important to note that windowAll() is NOT a parallel operation, meaning that all records will be gathered in one task.


Evictor or trigger will hand iterator (data belonging to a given logical window) to evaluation function. One can use either predefined functions for aggregation, 
such as sum(), min(), max(), ReduceFunction(), FoldFunction(). Finally, there is also a generic window function not surprisingly called WindowFunction(), a trait 
which one can further extend.


## Event time

[periodic watermark VS punctuated watermarks](http://stackoverflow.com/questions/41809228/watermarks-in-apache-flink)



## Rich Functions

Besides user-defined function (map, reduce, etc), rich functions provide four methods: open, close, getRuntimeContext, and setRuntimeContext.




## Notes: Data Sinks

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


## Testing

For easy testing Flink provides a third ExecutionEnvironment:

```{scala}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

// optionally define parallelism level for test simplification
val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 1)
```

Generating test data in the form of DataStream:

```{scala}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, DataStream}
import org.apache.flink.contrib.streaming.DataStreamUtils
import scala.collection.JavaConverters.asScalaIteratorConverter

// 1) preparing a raw text, in order to test transformation operations
val sampleText: DataStream[String] = env.fromElements("To be, or not to be,--that is the question:--",
    "Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune",
    "Or to take arms against a sea of troubles,")

// 2) prepare expected result to check in the test based on a collection (already partiotioned text)
val sampleSentence: Seq[(String, Int)] = Seq(("to", 2), ("be", 2), 
("or", 1), ("not", 1), ("that", 1), ("is", 1), ("the", 1), ("question", 1))
val sampleSentenceDataStream: DataStream[(String, Int)] = env.fromCollection(sampleSentence)

val myOutput: Iterator[(String, Int)] = DataStreamUtils.collect(sampleSentenceDataStream.javaStream).asScala
```

Note that in order to use org.apache.flink.contrib.streaming.DataStreamUtils you will need to add the following dependency in build.sbt:

```
"org.apache.flink" %% "flink-streaming-contrib" % "1.2.0" % "provided"
```

# Useful resources
Finally, some useful sources (most of them used as reference here):
- [Introduction to Apache Flink presentation from DataArtisans EIT ICT Summer School](http://ictlabs-summer-school.sics.se/2015/slides/flink-intro.pdf)
- [Flink Blog post introducing stream windows](https://flink.apache.org/news/2015/12/04/Introducing-windows.html)
- [periodic watermark VS punctuated watermarks](http://stackoverflow.com/questions/41809228/watermarks-in-apache-flink)