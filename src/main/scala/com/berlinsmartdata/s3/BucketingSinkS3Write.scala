package com.berlinsmartdata.s3

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink
import org.apache.flink.streaming.connectors.fs.{DateTimeBucketer}
import org.apache.flink.streaming.api.scala._


/**
  * Example shows basic read & write from/to S3
  *
  * Requires that user:
  *     a) starts a netcat session in the terminal - BEFORE running this code -
  *        via the following command:
  *        nc -lk 9999
  *
  *     b) in the same terminal window type messages, that will be aggregated
  *        by Flink
  *
  *     c) copy-pastes "core-site.xml" to "core-site.xml" located in
  *        directory src/main/resources/hadoop-config/  , AND enters
  *        AWS credentials (if you have done that already once previously,
  *        no need to repeat it)
  *
  *     d) specifies S3 Bucket for the val DEFAULT_S3_BUCKET;
  *
  */
object BucketingSinkS3Write {
  // DEFAULT_S3_BUCKET = YOUR-BUCKET-HERE (please substitute with your own bucket for testing purposes)
  lazy val DEFAULT_S3_BUCKET = "9-labs"

  def main(args: Array[String]): Unit = {

    val parameters = ParameterTool.fromArgs(args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    /**
      * config setup
      */
    env.getConfig.setGlobalJobParameters(parameters)
    // ONLY because we want to make things more comprehensive,
    // we set parallelism only to 1
    env.setParallelism(1)

    /**
      * Setup websocket source
      */
    //Create streams for names and ages by mapping the inputs to the corresponding objects
    val text = env.socketTextStream("localhost", 9999)

    val counts = mapOps(text)

    mapSink(counts)

    // execute program
    env.execute("Flink Scala - Windowed write to S3")

    // Note: once you terminate netcat session, Flink execution
    // will terminate gracefully and you'll be able to see S3 file on S3
  }

  def mapOps(data: DataStream[String]): DataStream[(String, Int)] = {
    val counts = data.flatMap {
      _.toLowerCase.split("\\W+").filter {
        _.nonEmpty
      }
    }
      .map {
        (_, 1)
      }
      .keyBy(0)
      .sum(1)
    counts.print()
    counts
  }

  /**
    * Data Sink: Partitioned write to File System/S3
    *
    * Note: Since Flink 1.2, BucketingSink substitutes
    *       RollingSink implementation
    *
    */
  def mapSink(data: DataStream[(String, Int)], path: String = s"s3://${DEFAULT_S3_BUCKET}/testBucketSink/") {
    val sink = new BucketingSink[(String, Int)](path)
    sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB - default is 384 MB
    data.addSink(sink)
  }
}
