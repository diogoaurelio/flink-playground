package com.berlinsmartdata.s3

import com.berlinsmartdata.sinks.AvroSinkWriter
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.fs.SequenceFileWriter


/**
  * Example shows Flink using HDFS Connector with BucketingSink, with final sink to S3
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

    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

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
    val text: DataStream[String] = env.socketTextStream("localhost", 9999)

    val counts = mapOps(text)

    mapSink(counts)
    // output to a second Sink 8in this case, same bucket, just different partitioning
    mapSink(path = s"s3://${DEFAULT_S3_BUCKET}/testBucketSink2/", data = counts)

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
  def mapSink(data: DataStream[(String, Int)], path: String = s"s3://${DEFAULT_S3_BUCKET}/testBucketingSink/") {
    // TODO: org.apache.avro.specific.SpecificRecordBase
    val avroSinkWriter = new AvroSinkWriter[(String, Int)]()

    val sink = new BucketingSink[(String, Int)](path)
    sink.setBucketer(new DateTimeBucketer[(String, Int)]("yyyy/MM/dd/HH"))
    sink.setInactiveBucketThreshold(60*60*1000) // 1h - timeout in milliseconds
    //sink.setWriter(new Writer[(String, Int)]())
    sink.setPendingPrefix("file-")
    sink.setPendingSuffix(".avro")
    sink.setBatchSize(1024 * 1024 * 128) // this is 128 MB - default is 384 MB
    sink.setWriter(avroSinkWriter)

    data.addSink(sink).setParallelism(1)
  }
}
