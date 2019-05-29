package com.datenn.s3

import com.datenn.model.WordCount
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scala.collection.JavaConversions._


/**
  * Example shows Window aggregation & write/sink to S3
  *
  * This will create and output results to single file in S3. Data
  * Streams written into file will be logically partitioned by a
  * time window
  *
  * More on Flink Window Functions:
  *       https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/windows.html
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
  *
  */
object WindowedFunctionS3Write {

  // DEFAULT_S3_BUCKET = YOUR-BUCKET-HERE (please substitute with your own bucket for testing purposes)
  lazy val DEFAULT_S3_BUCKET = "9-labs"

  def main(args: Array[String]): Unit = {

    val parameters = ParameterTool.fromArgs(args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    /**
      * Optionally add config setup
      */
    env.getConfig.setGlobalJobParameters(parameters)

    /**
      * Setup websocket source
      */
    //Create streams for names and ages by mapping the inputs to the corresponding objects
    val text = env.socketTextStream("localhost", 9999)

    val windowedCounts = mapOps(text)

    /**
      * Data Sink: Write back to S3 as a Datastream
      */
    mapSink(data = windowedCounts)

    // execute program
    env.execute("Flink Scala - Windowed write to S3")

    // Note: once you terminate netcat session, Flink execution
    // will terminate gracefully and you'll be able to see S3 file on S3;
    // do not kill Flink, otherwise you will not see output results S3
  }

  /**
    * Adds window aggregation
    * @param data
    * @param windowSize   seconds for window
    *                     aggregation
    * @return
    */
  def mapOps(data: DataStream[String], windowSize: Int = 3, offSet: Option[Int] = None): DataStream[WordCount] = {
    val counts = data.flatMap {
      _.toLowerCase.split("\\W+").filter {
        _.nonEmpty
      }
    }
      .map {
        WordCount(_, 1)
      }
      .keyBy(0)
      //.timeWindow(Time.seconds(windowSize))

      //.window(TumblingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(offSet.getOrElse(windowSize))))
      //.apply(new MyWindowFunction())
      //.reduce( (a, b) => a.add(b) )

      .sum(1)

    counts.print()
    counts
  }

  class MyWindowFunction extends WindowFunction[WordCount, String, String, TimeWindow] {
    // either use java.lang.Iterable or use:
    // import scala.collection.JavaConversions._
    override def apply(key: String,
                       window: TimeWindow,
                       input: java.lang.Iterable[WordCount],
                       out: Collector[String]): Unit = {
      var count = 0L
      for (in <- input) {
        count = count + 1
      }
      out.collect(s"Window $window count: $count")
    }
  }

  /**
    * Data Sink: Write back to S3 as a Datastream
    */
  def mapSink(data: DataStream[WordCount], path: String = s"s3://${DEFAULT_S3_BUCKET}/testWindowedWrite/text-$uuid.txt"): String = {
    data.writeAsText(path = path).setParallelism(1)
    path
  }

  private def uuid = java.util.UUID.randomUUID.toString

}
