package com.datenn.s3

import java.io.File

import com.datenn.model.WordCountWithTimeAvroFormat
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}
import com.datenn.testutils.FsTestUtils
import org.apache.flink.api.scala._


class EventTimeBucketingSinkS3WriteSpec extends FlatSpec
  with Matchers
  with BeforeAndAfterAll
  with BeforeAndAfterEach {

  val destinationPath = "/tmp/unitTestingFlink"
  val destinationDir: File = new File(destinationPath)
  val schema = WordCountWithTimeAvroFormat.SCHEMA$
  val yy = "2017"
  val mm = "04"
  val dd = "27"
  val targetPath = s"$destinationPath/$yy/$mm/$dd"
  val targetPath1 = s"$destinationPath/$yy/$mm/$dd/01"
  val targetPath2 = s"$destinationPath/$yy/$mm/$dd/02"
  val avroFile = "file-part-0-0.avro"
  val avroFileCrc = "file-part-0-0.crc"
  val targetFile1avro = s"$targetPath1/$avroFile"
  val targetFile1crc = s"$targetPath1/$avroFileCrc"

  val targetFile2avro = s"$targetPath2/$avroFile"
  val targetFile2crc = s"$targetPath2/$avroFileCrc"

  override def beforeAll(): Unit = {
    if(!destinationDir.exists)
      destinationDir.mkdir
  }

  override def afterEach(): Unit = {
    val avroRegex: scala.util.matching.Regex = s""".avro""".r
    FsTestUtils.cleanUp(avroRegex, targetPath1)
    FsTestUtils.cleanUp(avroRegex, targetPath2)
  }

  override def afterAll(): Unit = {
    if(destinationDir.exists)
      destinationDir.deleteOnExit
  }

  /** Finally Flink Env */
  trait FlinkTestEnv {
    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 1)
    lazy val hr1 = "01"
    lazy val hr2 = "02"
    lazy val dt = new DateTime(DateTime.parse(s"$yy-$mm-${dd}T$hr1:59:05Z"))
    lazy val unixTimeStamp: Long = dt.getMillis / 1000

    lazy val dt02 = new DateTime(DateTime.parse(s"$yy-$mm-${dd}T$hr1:01:05Z"))
    lazy val unixTimeStamp02: Long = dt02.getMillis / 1000

    lazy val dt03 = new DateTime(DateTime.parse(s"$yy-$mm-${dd}T$hr2:01:05Z"))
    lazy val unixTimeStamp03: Long = dt03.getMillis / 1000

    lazy val dt04 = new DateTime(DateTime.parse(s"$yy-$mm-${dd}T$hr2:59:05Z"))
    lazy val unixTimeStamp04: Long = dt04.getMillis / 1000

  }

  it should "write in same partition with Avro format for same event hour" in new FlinkTestEnv {
    val input1 = WordCountWithTimeAvroFormat("question", 1, unixTimeStamp, dt.toString)
    val input2 = WordCountWithTimeAvroFormat("question", 2, unixTimeStamp02, dt02.toString)
    val testData: Seq[WordCountWithTimeAvroFormat] = Seq(input1, input2)

    val testDataDs: DataStream[WordCountWithTimeAvroFormat] = env.fromCollection(testData)
    EventTimeBucketingSinkS3Write.mapSink(data = testDataDs, path = destinationPath)
    env.execute("Run test")
    val result = FsTestUtils.avroFileReader(targetFile1avro, schema)
    result shouldBe s"${input1}${input2}"
  }

  it should "write in distinct partitions with Avro format for different event hours" in new FlinkTestEnv {
    val input1 = WordCountWithTimeAvroFormat("question", 1, unixTimeStamp, dt.toString)
    val input2 = WordCountWithTimeAvroFormat("question", 2, unixTimeStamp03, dt03.toString)
    val testData: Seq[WordCountWithTimeAvroFormat] = Seq(input1, input2)

    val testDataDs: DataStream[WordCountWithTimeAvroFormat] = env.fromCollection(testData)
    EventTimeBucketingSinkS3Write.mapSink(data = testDataDs, path = destinationPath)
    env.execute("Run test")
    val result1 = FsTestUtils.avroFileReader(targetFile1avro, schema)
    result1 shouldBe s"${input1}"

    val result2 = FsTestUtils.avroFileReader(targetFile2avro, schema)
    result2 shouldBe s"${input2}"
  }

  it should "write in correct partitions with Avro format for several hours" in new FlinkTestEnv {

    val input1 = WordCountWithTimeAvroFormat("question", 1, unixTimeStamp, dt.toString)
    val input2 = WordCountWithTimeAvroFormat("question", 2, unixTimeStamp03, dt03.toString)
    val input3 = WordCountWithTimeAvroFormat("question", 2, unixTimeStamp02, dt02.toString)
    val input4 = WordCountWithTimeAvroFormat("question", 1, unixTimeStamp04, dt04.toString)
    val testData: Seq[WordCountWithTimeAvroFormat] = Seq(input4, input1, input2, input3)

    val testDataDs: DataStream[WordCountWithTimeAvroFormat] = env.fromCollection(testData)
    EventTimeBucketingSinkS3Write.mapSink(data = testDataDs, path = destinationPath)
    env.execute("Run test")
    val result1 = FsTestUtils.avroFileReader(targetFile1avro, schema)
    result1 shouldBe s"${input1}${input3}"

    val result2 = FsTestUtils.avroFileReader(targetFile2avro, schema)
    result2 shouldBe s"${input4}${input2}"
  }


}
