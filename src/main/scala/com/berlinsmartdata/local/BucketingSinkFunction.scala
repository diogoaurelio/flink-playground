package com.berlinsmartdata.local

import org.apache.flink.streaming.api.scala._

/**
  * Example shows basic read & write from/to local file system
  * based on manually built dataset
  *
  * Note: this example is useful for unit testing purposes
  */

object BucketingSinkFunction {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.createLocalEnvironment()

    /**
      * Create artificially dataset
      */
    val text = env.fromElements("To be, or not to be,--that is the question:--",
      "Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune",
      "Or to take arms against a sea of troubles,")


    val ints = env.fromElements(1, 2, 3, 4, 5)
    //val countInts = ints.filter(x => x % 2 == 0).sum(0)

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
      * Data Sink: Write back to filesystem as a Datastream
      */
    counts.writeAsText(s"/tmp/${this.getClass.getCanonicalName}/test-${uuid}.txt")

    //    val sink = new BucketingSink[String](s"/tmp/${this.getClass}/test.txt")
    //    sink.setBucketer(new DateTimeBucketer[String]("yyyy-MM-dd--HHmm"))
    //    sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB,
    //
    //    input.addSink(sink)

    //    val sink = new BucketingSink[String]("/base/path")
    //    sink.setBucketer(new Bucketer("yyyy-MM-dd--HHmm"))
    //    //sink.setWriter(new SequenceFileWriter[IntWritable, Text]())
    //    sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB - the default part file size is 384 MB


    // execute program
    env.execute("Flink Scala - Basic read & write to filesystem")

  }

  private def uuid = java.util.UUID.randomUUID.toString

}
