package com.berlinsmartdata.s3

import com.berlinsmartdata.model.WordCountWithTimeAvroFormat
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import java.io.{ByteArrayOutputStream, File, IOException}

import com.berlinsmartdata.testutils.FsTestUtils
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord, GenericRecordBuilder}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.SchemaBuilder
import org.apache.hadoop.fs.Path
import org.apache.avro.Schema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.AvroTypeInfo
import org.apache.flink.api.scala._


class EventTimeBucketingSinkS3WriteSpec extends FlatSpec with Matchers {

  lazy val destinationPath = "/tmp/unitTestingFlink"
  lazy val schema = WordCountWithTimeAvroFormat.SCHEMA$

  /** Finally Flink Env */
  trait FlinkTestEnv {
    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 1)
    lazy val hr = "01"
    lazy val dt = new DateTime(DateTime.parse(s"2017-04-27T$hr:59:05Z"))
    lazy val unixTimeStamp: Long = dt.getMillis / 1000

    lazy val dt02 = new DateTime(DateTime.parse(s"2017-04-27T$hr:01:05Z"))
    lazy val unixTimeStamp02: Long = dt02.getMillis / 1000

  }

  it should "write in same bucket with Avro format for same hour" in new FlinkTestEnv {
    val input1 = WordCountWithTimeAvroFormat("question", 1, unixTimeStamp, dt.toString)
    val input2 = WordCountWithTimeAvroFormat("question", 2, unixTimeStamp02, dt02.toString)
    val testData: Seq[WordCountWithTimeAvroFormat] = Seq(input1, input2)

    val testDataDs: DataStream[WordCountWithTimeAvroFormat] = env.fromCollection(testData)
    EventTimeBucketingSinkS3Write.mapSink(data = testDataDs, path = destinationPath)
    env.execute("Run test")
    val targetFolder = s"$destinationPath/2017/04/27/01/file-part-0-0.avro"
    val result = FsTestUtils.avroFileReader(targetFolder, schema)
    result shouldBe s"${input1}${input2}"

  }

  it should "Save files into file system successfully" in new FlinkTestEnv {

    val testData: Seq[(String, Int)] = Seq(("to", 1), ("be", 1),
      ("or", 1), ("not", 1), ("to", 2), ("be", 2), ("that", 1), ("is", 1),
      ("the", 1), ("question", 1))
//    val testDataDS: DataStream[(String, Int)] = env.fromCollection(testData)
//
//    val destPath: String = BasicS3ReadWrite.mapSink(data = testDataDS, path = destinationPath)
//    env.execute("Run test")
//
//    val resultAsSeq = scala.io.Source.fromFile(destPath).getLines.toIndexedSeq

    //resultAsSeq shouldEqual(expectedTestCollection)
  }


}
