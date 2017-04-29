package com.berlinsmartdata.s3


import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import com.berlinsmartdata.model.{WordCount, WordCountWithTime}
import com.berlinsmartdata.sinks.{EventTimeBucketer, WordCountTimeBucketer}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.joda.time._

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
object WindowedBucketingSinkS3Write {
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
    // set event time processing
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    /**
      * Setup websocket source
      */
    //Create streams for names and ages by mapping the inputs to the corresponding objects
    val text: DataStream[String] = env.socketTextStream("localhost", 9999)

    val counts = mapOps(text)

    mapSink(counts)
    // Note: oyu can add how many sinks you want: in this case, we are
    // outputing same result to a second Sink (same bucket, just different partitioning)
    mapSink(path = s"s3://${DEFAULT_S3_BUCKET}/testBucketSink2/", data = counts)

    // execute program
    env.execute("Flink Scala - Windowed write to S3")

    // Note: once you terminate netcat session, Flink execution
    // will terminate gracefully and you'll be able to see S3 file on S3
  }

  def mapOps(data: DataStream[String]): DataStream[WordCountWithTime] = {
    val counts = data.flatMap {
        _.toLowerCase.split("\\W+").filter {
          _.nonEmpty
        }
      }
      .map { s =>
        val hr = scala.util.Random.nextInt(12)
        val mm = scala.util.Random.nextInt(60)
        val dt = new DateTime(DateTime.parse(s"2017-04-27T$hr:$mm:05Z"))
        val unixTimeStamp: Long = dt.getMillis / 1000
        WordCountWithTime(s, 1, unixTimeStamp, dt)
      }

    val withTimestampsAndWatermarks = counts.assignTimestampsAndWatermarks(new OutOfOrdernessDelayWatermark())

    val timedCounts = withTimestampsAndWatermarks
      .keyBy(0,2)
      .timeWindow(Time.days(1))
      .sum(1)

    timedCounts.print()
    timedCounts
  }



  /**
    * Data Sink: Partitioned write to File System/S3
    *
    * Note: Since Flink 1.2, BucketingSink substitutes
    *       RollingSink implementation
    *
    */
  def mapSink(data: DataStream[WordCountWithTime], path: String = s"s3://${DEFAULT_S3_BUCKET}/testWindowedBucketingSink/") {

    val sink = new BucketingSink[WordCountWithTime](path)
    //sink.setBucketer(new DateTimeBucketer[WordCountWithTime]("yyyy/MM/dd/HH/mm"))
    sink.setBucketer(new WordCountTimeBucketer[WordCountWithTime])

    sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB - default is 384 MB
    sink.setInactiveBucketThreshold(60*60*1000) // 1h - timeout in milliseconds
    sink.setPendingPrefix("file-")
    sink.setPendingSuffix(".avro")
    data.addSink(sink).setParallelism(1)

  }
}

class OutOfOrdernessDelayWatermark extends BoundedOutOfOrdernessTimestampExtractor[WordCountWithTime](Time.seconds(3600)) {
  override def extractTimestamp(element: WordCountWithTime): Long = element.time
}