package com.datenn.s3

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import com.datenn.model.WordCount

import scala.collection.JavaConverters.asScalaIteratorConverter
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import java.io.{BufferedReader, DataInputStream, InputStream, InputStreamReader}

import com.datenn.testutils.TestCollector
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
import scala.util.control
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

/**
  * Note: this Spec also REQUIRES to have environment
  *       variable HADOOP_CONF_DIR set to /{YOUR-PATH-TO-THIS-REPO}/playground/src/main/resources/hadoop-config/
  */
class WindowedFunctionS3WriteSpec extends FlatSpec
  with Matchers
  with BeforeAndAfterAll {

  import com.datenn.testutils.FsTestUtils._

  override def beforeAll(): Unit = {
    super.beforeAll()
    initiS3
  }

  // file destination path
  lazy val destinationPath = "/tmp/unitTestingFlinkWindowedFunction/shakespeare-text.txt"
  lazy val defaultS3TestBucket = "s3://9-labs"

  /** Common expected test result */
  lazy val expectedTestCollection: Seq[String] = Seq("(to,1)", "(be,1)",
    "(or,1)", "(not,1)", "(to,2)", "(be,2)", "(that,1)", "(is,1)",
    "(the,1)", "(question,1)")


  def now_ms = System.currentTimeMillis()

  /** Finally Flink Env */
  trait FlinkTestEnv {
    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 1)
    //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    // alternatively:
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    lazy val out = new TestCollector[String]
  }

  def sourceFF(scx: SourceContext[Int]): Unit = {
    var cur = 1
    var now: Long = now_ms
    while (cur < 31) {
      // every 10 wait 10 seconds and then burst a bunch
      if (cur % 10 == 0) {
        Thread.sleep(10000)
        now = now_ms
      }
      println("emiting: " + cur + ", " + now)
      scx.collectWithTimestamp(cur, now)
      cur += 1
    }
  }

  /**
    * Tests start here
    */

  it should "test a window function" in new FlinkTestEnv {
    val textSample01 = env.fromElements("question question question")
    val windowSize = 1

    val windowedStream = textSample01
      .flatMap{ _.toLowerCase.split("\\W+") }
      .map{ (_, 1) }
      .keyBy(0)
      //.window(TumblingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSize)))
      //.apply(new MyWindowFunction())
      .sum(1)
  }

  it should "Count words in a DataStream in method mapOps" in new FlinkTestEnv {
    //.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val textSample01 = env.fromElements("question question question")
    val expectedCollection: Seq[WordCount] = Seq(WordCount("question", 1),
      WordCount("question", 2), WordCount("question", 3))
    val windowSize = 2

    val result: DataStream[WordCount] = WindowedFunctionS3Write.mapOps(data = textSample01, windowSize)

    env.execute("Run test")

    val resultAsSeq = DataStreamUtils.collect(result.javaStream).asScala.toIndexedSeq
    //Thread.sleep(2)
    resultAsSeq shouldEqual(expectedCollection)
  }

  it should "count only words in method mapOps" in new FlinkTestEnv {
    val textSample01 = env.fromElements("To be, or not to be,--that is the question:--")

    val expectedCollection: Seq[(String, Int)] = Seq(("to", 1), ("be", 1),
      ("or", 1), ("not", 1), ("to", 2), ("be", 2), ("that", 1), ("is", 1),
      ("the", 1), ("question", 1))

    val result: DataStream[WordCount] = WindowedFunctionS3Write.mapOps(data = textSample01)
    env.execute("Run test")

    val resultAsSeq = DataStreamUtils.collect(result.javaStream).asScala.toIndexedSeq

    resultAsSeq shouldEqual(expectedCollection)
  }

  it should "Save files into file system successfully" in new FlinkTestEnv {
    cleanUpFilesTestHelper(destinationPath)

    val testData: Seq[WordCount] = Seq(WordCount("to", 1), WordCount("be", 1),
      WordCount("or", 1), WordCount("not", 1), WordCount("to", 2), WordCount("be", 2),
      WordCount("that", 1), WordCount("is", 1),
      WordCount("the", 1), WordCount("question", 1))
    val testDataDS: DataStream[WordCount] = env.fromCollection(testData)

    val destPath: String = WindowedFunctionS3Write.mapSink(data = testDataDS, path = destinationPath)
    env.execute("Run test")

    val resultAsSeq = scala.io.Source.fromFile(destPath).getLines.toIndexedSeq

    resultAsSeq shouldEqual(expectedTestCollection)
  }

  /**
    * Integration Test - save files to S3 using local credentials
    */
  it should "Save files into AWS S3 successfully" in new FlinkTestEnv {
    val targetFile = s"$defaultS3TestBucket/testWindowedWriteToS3/test.txt"
    deleteS3File(targetFile)

    val testData: Seq[WordCount] = Seq(WordCount("to", 1), WordCount("be", 1),
      WordCount("or", 1), WordCount("not", 1), WordCount("to", 2), WordCount("be", 2),
      WordCount("that", 1), WordCount("is", 1),
      WordCount("the", 1), WordCount("question", 1))
    val testDataDS: DataStream[WordCount] = env.fromCollection(testData)

    // Note: by default uses AWS S3
    val destPath: String = WindowedFunctionS3Write.mapSink(data = testDataDS,
      path = targetFile)

    env.execute("Run test")

    val result = getS3File(destPath)
    parseInputStream(result, expectedTestCollection)
  }


  /** Integration test - test all main methods in job */

  it should "transform and write successfully data" in new FlinkTestEnv {
    cleanUpFilesTestHelper(destinationPath)

    val textSample01 = env.fromElements("To be, or not to be,--that is the question:--")

    val transformedText: DataStream[WordCount] = WindowedFunctionS3Write.mapOps(data = textSample01)

    val filePath = WindowedFunctionS3Write.mapSink(path = destinationPath, data = transformedText)

    env.execute("Run test")

    val resultAsSeq = scala.io.Source.fromFile(filePath).getLines.toIndexedSeq
    resultAsSeq shouldEqual(expectedTestCollection)
  }

}
