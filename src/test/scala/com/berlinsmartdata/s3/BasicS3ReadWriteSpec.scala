package com.berlinsmartdata.s3

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.contrib.streaming.DataStreamUtils

import scala.collection.JavaConverters.asScalaIteratorConverter
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}
import java.io.{BufferedReader, InputStream, InputStreamReader}
import com.berlinsmartdata.testutils.FsTestUtils

/**
  * Note: this Spec also REQUIRES to have environment
  *       variable HADOOP_CONF_DIR set to /{YOUR-PATH-TO-THIS-REPO}/playground/src/main/resources/hadoop-config/
  */
class BasicS3ReadWriteSpec extends FlatSpec
  with Matchers
  with BeforeAndAfterEach
  with BeforeAndAfterAll {

  import com.berlinsmartdata.testutils.FsTestUtils._

  override def beforeAll(): Unit = {
    super.beforeAll()
    FsTestUtils.initiS3
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    cleanUpFilesTestHelper()
  }

  // file destination path
  lazy val destinationPath = "/tmp/unitTestingFlink/shakespeare-text.txt"
  lazy val defaultS3TestBucket = "s3://9-labs"

  /** Common expected test result */
  lazy val expectedTestCollection: Seq[String] = Seq("(to,1)", "(be,1)",
    "(or,1)", "(not,1)", "(to,2)", "(be,2)", "(that,1)", "(is,1)",
    "(the,1)", "(question,1)")


  /** Finally Flink Env */
  trait FlinkTestEnv {
    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 1)
  }

  /**
    * Parses inputStream and asserts it matches expected results
    * @param result
    * @param expectedCollection
    */
  def parseInputStream(result: InputStream, expectedCollection: Seq[String]): Unit = {
    val reader = new BufferedReader(new InputStreamReader(result))
    var counter = 0
    while(reader.ready) {
      val line = reader.readLine
      if (line != null) {
        println("Line is: "+line)
        line shouldBe expectedCollection(counter)
        counter += 1
      }
    }
  }

  def cleanUpFilesTestHelper(path: String = destinationPath) {
    if (new java.io.File(path).exists) {
      try {
        new java.io.File(path).delete()
      } catch {
        case e: Exception => println(s"Failed to cleanup files after test: ${e.getMessage}")
      }
    }
  }

  /**
    * Tests start here
    */

  it should "Count words in a DataStream in method mapOps" in new FlinkTestEnv {

    val textSample01 = env.fromElements("question question question")
    val expectedCollection: Seq[(String, Int)] = Seq(("question", 1), ("question", 2), ("question", 3))

    val result: DataStream[(String, Int)] = BasicS3ReadWrite.mapOps(data = textSample01)
    env.execute("Run test")

    val resultAsSeq = DataStreamUtils.collect(result.javaStream).asScala.toIndexedSeq
    resultAsSeq shouldEqual(expectedCollection)
  }

  it should "count only words in method mapOps" in new FlinkTestEnv {
    val textSample01 = env.fromElements("To be, or not to be,--that is the question:--")

    val expectedCollection: Seq[(String, Int)] = Seq(("to", 1), ("be", 1),
      ("or", 1), ("not", 1), ("to", 2), ("be", 2), ("that", 1), ("is", 1),
      ("the", 1), ("question", 1))

    val result: DataStream[(String, Int)] = BasicS3ReadWrite.mapOps(data = textSample01)
    env.execute("Run test")

    val resultAsSeq = DataStreamUtils.collect(result.javaStream).asScala.toIndexedSeq

    resultAsSeq shouldEqual(expectedCollection)
  }

  it should "Save files into file system successfully" in new FlinkTestEnv {
    cleanUpFilesTestHelper(destinationPath)

    val testData: Seq[(String, Int)] = Seq(("to", 1), ("be", 1),
      ("or", 1), ("not", 1), ("to", 2), ("be", 2), ("that", 1), ("is", 1),
      ("the", 1), ("question", 1))
    val testDataDS: DataStream[(String, Int)] = env.fromCollection(testData)

    val destPath: String = BasicS3ReadWrite.mapSink(data = testDataDS, path = destinationPath)
    env.execute("Run test")

    val resultAsSeq = scala.io.Source.fromFile(destPath).getLines.toIndexedSeq

    resultAsSeq shouldEqual(expectedTestCollection)
  }

  /**
    * Integration Test - save files to S3 using local credentials
    */
  it should "Save files into AWS S3 successfully" in new FlinkTestEnv {
    val targetFile = s"$defaultS3TestBucket/testWriteToS3/test.txt"
    deleteS3File(targetFile)

    val testData: Seq[(String, Int)] = Seq(("to", 1), ("be", 1),
      ("or", 1), ("not", 1), ("to", 2), ("be", 2), ("that", 1), ("is", 1),
      ("the", 1), ("question", 1))
    val testDataDS: DataStream[(String, Int)] = env.fromCollection(testData)

    // Note: by default uses AWS S3
    val destPath: String = BasicS3ReadWrite.mapSink(data = testDataDS,
      path = targetFile)

    env.execute("Run test")

    val result = getS3File(destPath)
    parseInputStream(result, expectedTestCollection)
  }


  /** Integration test - test all main methods in job */

  it should "transform and write successfully data" in new FlinkTestEnv {
    cleanUpFilesTestHelper(destinationPath)

    val textSample01 = env.fromElements("To be, or not to be,--that is the question:--")

    val transformedText: DataStream[(String, Int)] = BasicS3ReadWrite.mapOps(data = textSample01)

    val filePath = BasicS3ReadWrite.mapSink(path = destinationPath, data = transformedText)

    env.execute("Run test")

    val resultAsSeq = scala.io.Source.fromFile(filePath).getLines.toIndexedSeq
    resultAsSeq shouldEqual(expectedTestCollection)
  }


}