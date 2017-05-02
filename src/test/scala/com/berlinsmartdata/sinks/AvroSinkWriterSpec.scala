package com.berlinsmartdata.sinks

import com.berlinsmartdata.model.WordCountWithTimeAvroFormat
import com.berlinsmartdata.testutils.FsTestUtils
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.File
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class AvroSinkWriterSpec extends FlatSpec with Matchers with BeforeAndAfterEach {

  lazy val schema: Schema = WordCountWithTimeAvroFormat.SCHEMA$
  lazy val destinationPath: String = "/tmp/AvroSinkWriterSpec"
  lazy val avroFile = "testAvro.avro"
  lazy val avroFileCrc = ".testAvro.avro.crc"
  lazy val finalTarget = s"$destinationPath/$avroFile"

  override def beforeEach(): Unit = {
    super.beforeEach()
    FsTestUtils.cleanUpFilesTestHelper(finalTarget)
    FsTestUtils.cleanUpFilesTestHelper(s"$destinationPath/$avroFileCrc")
  }

  def initializeWriter(schema: Schema = schema, targetPath: String = finalTarget): AvroSinkWriter[WordCountWithTimeAvroFormat] = {
    val avroSinkWriter = new AvroSinkWriter[WordCountWithTimeAvroFormat]
    avroSinkWriter.setSchema(schema)

    val fs = FileSystem.get(new Configuration())
    val path = new Path(targetPath)

    avroSinkWriter.open(fs, path)
    avroSinkWriter
  }

  "FsTestUtils.avroFileReader" should "be able to properly read Avro Files" in {
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val dataFileWriter = new DataFileWriter[GenericRecord](writer)
    dataFileWriter.create(schema, new File(finalTarget))

    val input1 = WordCountWithTimeAvroFormat("hello", 1, 1L, "date")
    val input2 = WordCountWithTimeAvroFormat("world", 1, 1L, "date")
    dataFileWriter.append(input1)
    dataFileWriter.append(input2)
    dataFileWriter.close

    val result = FsTestUtils.avroFileReader(finalTarget, schema)
    result shouldBe s"${input1}${input2}"
  }

  it should "write WordCount to Avro file" in {

    val avroSinkWriter = initializeWriter()
    val input = WordCountWithTimeAvroFormat("word", 1, 1L, "date")
    avroSinkWriter.write(input)
    avroSinkWriter.close()

    val result = FsTestUtils.avroFileReader(finalTarget, schema)
    result shouldBe s"${input}"
  }


  it should "append several data into Avro file" in {

    val avroSinkWriter = initializeWriter()
    val input1 = WordCountWithTimeAvroFormat("hello", 1, 1L, "date")
    val input2 = WordCountWithTimeAvroFormat("world", 1, 1L, "date")
    avroSinkWriter.write(input1)
    avroSinkWriter.write(input2)
    avroSinkWriter.close()

    val result = FsTestUtils.avroFileReader(finalTarget, schema)
    result shouldBe s"${input1}${input2}"
  }


}
