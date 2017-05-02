package com.berlinsmartdata.s3

import java.io.ByteArrayOutputStream

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import com.berlinsmartdata.model.{DataSetError, WordCount, WordCountWithTime, WordCountWithTimeAvroFormat}
import com.berlinsmartdata.sinks.{AvroSinkWriter, EventTimeBucketer, WordCountTimeBucketerViaReflection}
import org.apache.avro.{Schema, specific}
import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.{SpecificData, SpecificDatumWriter, SpecificRecordBase}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.joda.time._

import scala.collection.mutable.HashMap
import scala.reflect.ClassTag


/**
  * Example shows Flink using HDFS Connector with BucketingSink,
  * along with Window Function with final sink to S3
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
object EventTimeBucketingSinkS3Write {
  // DEFAULT_S3_BUCKET = YOUR-BUCKET-HERE (please substitute with your own bucket for testing purposes)
  lazy val DEFAULT_S3_BUCKET = "9-labs"

  def main(args: Array[String]): Unit = {

    val parameters = ParameterTool.fromArgs(args)

    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    /**
      * config setup
      */
    env.getConfig.setGlobalJobParameters(parameters)
    env.setParallelism(10)
    // ONLY because we want to make things more comprehensive,
    // set event time processing
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    /**
      * Setup websocket source
      */
    //Create streams for names and ages by mapping the inputs to the corresponding objects
    val text: DataStream[String] = env.socketTextStream("localhost", 9999)

    val counts = mapOps(text)

    mapSink(counts)
    // execute program
    env.execute("Flink Scala - Windowed write to S3")

    // Note: once you terminate netcat session, Flink execution
    // will terminate gracefully and you'll be able to see S3 file on S3
  }

  def mapOps(data: DataStream[String]): DataStream[WordCountWithTimeAvroFormat] = {
    val counts = data.flatMap {
        _.toLowerCase.split("\\W+").filter {
          _.nonEmpty
        }
      }
      .map { s =>
        val hr = scala.util.Random.nextInt(2)
        val mm = scala.util.Random.nextInt(10)
        val dt = new DateTime(DateTime.parse(s"2017-04-27T$hr:$mm:05Z"))
        val unixTimeStamp: Long = dt.getMillis / 1000
        WordCountWithTimeAvroFormat(s, 1, unixTimeStamp, dt.toString)
      }
      .keyBy(0,2)
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
  def mapSink(data: DataStream[WordCountWithTimeAvroFormat], path: String = s"s3://${DEFAULT_S3_BUCKET}/testEventBucketingSink/") {

    val sink = new BucketingSink[WordCountWithTimeAvroFormat](path)
    sink.setBucketer(new WordCountTimeBucketerViaReflection[WordCountWithTimeAvroFormat])
    sink.setWriter(new AvroSinkWriter[WordCountWithTimeAvroFormat])
    sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB - default is 384 MB
    sink.setInactiveBucketThreshold(60*60*1000) // 1h - timeout in milliseconds
    sink.setPendingPrefix("file-")
    sink.setPendingSuffix(".avro")
    data.addSink(sink).setParallelism(1)
  }


}