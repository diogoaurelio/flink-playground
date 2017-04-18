package com.berlinsmartdata.s3

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._


/**
  * Example shows example Window aggregation & write to S3
  *
  * Requires that user specifies:
  *     a) S3 Bucket for the val DEFAULT_S3_BUCKET;
  *     b) Uploads the file "flink-basic-read-from-s3.txt"
  *        to the previous specified bucket (available in
  *        src/main/resources/)
  *     c) copy-pastes "core-site.xml" to "core-site.xml" located in
  *        directory src/main/resources/hadoop-config/  , AND enters
  *        AWS credentials
  *
  *
  */
object WindowFunctionS3ReadWrite {

  // DEFAULT_S3_BUCKET = YOUR-BUCKET-HERE (please substitute with your own bucket for testing purposes)
  lazy val DEFAULT_S3_BUCKET = "9-labs"
  lazy val DEFAULT_INPUT_FILE_NAME = "flink-basic-read-from-s3.txt"
  lazy val DEFAULT_OUTPUT_FILE_NAME = "flink-basic-write-to-s3"

  def main(args: Array[String]): Unit = {

    val parameters = ParameterTool.fromArgs(args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    /**
      * config setup
      */
    env.getConfig.setGlobalJobParameters(parameters)
    env.setParallelism(1)


    /**
      * To test this part
      */
    //Create streams for names and ages by mapping the inputs to the corresponding objects
    val text = env.socketTextStream("localhost", 9999)


    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(0)
      .sum(1)

    counts print

    counts.writeAsText(s"s3://${DEFAULT_S3_BUCKET}/${DEFAULT_OUTPUT_FILE_NAME}-${uuid}.txt")
    
    // execute program
    env.execute("Flink Scala - Basic write to S3")

  }

  def parseMap(line : String): (String, String) = {
    val record = line.substring(1, line.length - 1).split(",")
    (record(0), record(1))
  }

  def uuid = java.util.UUID.randomUUID.toString

}
