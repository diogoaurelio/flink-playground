package com.datenn.local

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink

/**
  * Example shows TODO
  *
  * Note: this example is useful for unit testing purposes
  */

object WindowedFunctionS3Write {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.createLocalEnvironment()

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
    val windowStream = wordsStream.keyBy(0).timeWindow(Time.seconds(1))
    // finally specify how to aggregate the streams, in this case by the right
    // side of the tuple (word, 1)
    val countStream = windowStream.sum(1)

    /**
      * Data Sink: Partitioned write to S3
      *
      * Note: Since Flink 1.2, BucketingSink substitutes
      *       RollingSink implementation
      *
      */

    val sink = new BucketingSink[(String, Int)](s"/tmp/testWindowedBucketSink/")
    sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB - default is 384 MB

    countStream.addSink(sink)


    // execute program
    env.execute("Flink Scala - Basic read & write to filesystem")

  }

}
