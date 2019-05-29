package com.datenn.s3

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.datastream.DataStreamUtils

import scala.collection.JavaConverters.asScalaIteratorConverter
import org.scalatest.{FlatSpec, Matchers}


class BucketingSinkS3WriteSpec extends FlatSpec with Matchers {

  val env = StreamExecutionEnvironment.createLocalEnvironment(1)

  val sampleSentence: Seq[(String, Int)] = Seq(("to", 2), ("be", 2),
    ("or", 1), ("not", 1), ("that", 1), ("is", 1), ("the", 1), ("question", 1))

  val sampleSentenceDataStream: DataStream[(String, Int)] = env.fromCollection(sampleSentence)

  def collectOutput(data: DataStream[(String, Int)]): Iterator[(String, Int)] = {
    DataStreamUtils.collect(data.javaStream).asScala
  }


  it should "count how many times each word appears" in {
    val text: DataStream[String] = env.fromElements("To be, or not to be,--that is the question:--",
      "Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune",
      "Or to take arms against a sea of troubles,")

    val result = BucketingSinkS3Write.mapOps(data = text)
    env.execute("Run test")
    val output = collectOutput(result)
    println(output)
    output shouldEqual(sampleSentence)

  }

  it should "run more than one job" in {

    val text: DataStream[String] = env.fromElements("To be, or not to be,--that is the question:--",
      "Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune",
      "Or to take arms against a sea of troubles,")

    val result = BucketingSinkS3Write.mapOps(data = text)
    env.execute("Run test")
    val output = collectOutput(result)
    println(output)
    output shouldEqual(sampleSentence)

  }



}

