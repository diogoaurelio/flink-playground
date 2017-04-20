package com.berlinsmartdata.local

import org.apache.flink.streaming.api.scala._

/**
  * Example shows basic read & write from/to local file system
  * based on manually built dataset
  *
  * Note: this example is useful for unit testing purposes
  */

object BasicReadWrite {


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
      * Data Sink: Write back to filesystem as a Datastream
      */
    counts.writeAsText(s"/tmp/myFlinkTest/test-${uuid}.txt")

    // execute program
    env.execute("Flink Scala - Basic read & write to filesystem")

  }

  private def uuid = java.util.UUID.randomUUID.toString

}
