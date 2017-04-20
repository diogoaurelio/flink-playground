package com.berlinsmartdata.s3

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


/**
  * Example shows example Tumbling Window aggregation & write to S3
  * More on Flink Window Functions:
  *       https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/windows.html
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
  *
  */
object WindowedFunctionS3Write {

  // DEFAULT_S3_BUCKET = YOUR-BUCKET-HERE (please substitute with your own bucket for testing purposes)
  lazy val DEFAULT_S3_BUCKET = "9-labs"

  def main(args: Array[String]): Unit = {

    val parameters = ParameterTool.fromArgs(args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    /**
      * config setup
      */
    env.getConfig.setGlobalJobParameters(parameters)
    env.setParallelism(1)


    /**
      * Setup websocket source
      */
    //Create streams for names and ages by mapping the inputs to the corresponding objects
    val text = env.socketTextStream("localhost", 9999)


    val wordsStream = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }

    // Here we create a Tumbling Window, where the stream is discretized
    // into non-overlapping windows split by a given time interval, and
    // using the left side of tuple (word, 1) as a key
    val windowStream = wordsStream.keyBy(0).timeWindow(Time.seconds(3))
    // finally specify how to aggregate the streams, in this case by the right
    // side of the tuple (word, 1)
    val countStream = windowStream.sum(1)

    /**
      * Data Sink: Write back to S3 as a Datastream
      */
    countStream.writeAsText(s"s3://${DEFAULT_S3_BUCKET}/testWindowedBucketSink/")
    
    // execute program
    env.execute("Flink Scala - Windowed write to S3")

    // Note: once you terminate netcat session, Flink execution
    // will terminate gracefully and you'll be able to see S3 file on S3

  }

  private def uuid = java.util.UUID.randomUUID.toString

}
