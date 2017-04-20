package com.berlinsmartdata.s3

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink
import org.apache.flink.streaming.connectors.fs.{DateTimeBucketer}
import org.apache.flink.streaming.api.scala._



object RollingSinkS3Write {
  // DEFAULT_S3_BUCKET = YOUR-BUCKET-HERE (please substitute with your own bucket for testing purposes)
  lazy val DEFAULT_S3_BUCKET = "9-labs"
  lazy val DEFAULT_OUTPUT_FILE_NAME = "flink-websocketstream-write-to-s3"

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
      *
      * Note: Since Flink 1.2, BucketingSink substitutes
      *       RollingSink implementation
      *
      */
    val hdfsSink = new BucketingSink[String]("s3://${DEFAULT_S3_BUCKET}/${DEFAULT_OUTPUT_FILE_NAME}-${uuid}.txt")
    //hdfsSink.setBucketer(new DateTimeBucketer("yyyy-MM-dd--HHmm"))

    countStream.writeAsText(s"s3://${DEFAULT_S3_BUCKET}/${DEFAULT_OUTPUT_FILE_NAME}-${uuid}.txt")

    // execute program
    env.execute("Flink Scala - Windowed write to S3")

    // Note: once you terminate netcat session, Flink execution
    // will terminate gracefully and you'll be able to see S3 file on S3

  }

  private def parseMap(line : String): (String, String) = {
    val record = line.substring(1, line.length - 1).split(",")
    (record(0), record(1))
  }

  private def uuid = java.util.UUID.randomUUID.toString

}
