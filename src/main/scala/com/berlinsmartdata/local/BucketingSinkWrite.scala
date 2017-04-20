package com.berlinsmartdata.local

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink

/**
  * Example shows basic read & write from/to local file system
  * based on manually built dataset
  *
  * Note: this example is useful for unit testing purposes
  */

object BucketingSinkWrite {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.createLocalEnvironment()

    /**
      * Create artificially dataset
      */
    val text = env.fromElements("To be, or not to be,--that is the question:--",
      "Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune",
      "Or to take arms against a sea of troubles,")

    val counts = text.flatMap {
      _.toLowerCase.split("\\W+") filter {
        _.nonEmpty
      }
    }
      .map {
        (_, 1)
      }
      .keyBy(0)
      .sum(1)

    counts print

    /**
      * Data Sink: Partitioned write to S3
      *
      * Note: Since Flink 1.2, BucketingSink substitutes
      *       RollingSink implementation
      *
      */

    val sink = new BucketingSink[(String, Int)](s"/tmp/testBucketSink/")
    sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB - default is 384 MB

    counts.addSink(sink)


    // execute program
    env.execute("Flink Scala - Basic read & write to filesystem")

  }

  private def uuid = java.util.UUID.randomUUID.toString

}
