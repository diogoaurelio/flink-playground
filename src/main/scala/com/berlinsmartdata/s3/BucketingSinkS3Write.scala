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


    val wordsStream = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }


    // finally specify how to aggregate the streams, in this case by the right
    // side of the tuple (word, 1)
    val countStream = wordsStream.keyBy(0).sum(1)

    /**
      * Data Sink: Partitioned write to S3
      *
      * Note: Since Flink 1.2, BucketingSink substitutes
      *       RollingSink implementation
      *
      */
    val sink = new BucketingSink[(String, Int)](s"s3://${DEFAULT_S3_BUCKET}/testBucketSink/")

    countStream.addSink(sink)

    // execute program
    env.execute("Flink Scala - Windowed write to S3")

    // Note: once you terminate netcat session, Flink execution
    // will terminate gracefully and you'll be able to see S3 file on S3

  }

  private def uuid = java.util.UUID.randomUUID.toString

}
