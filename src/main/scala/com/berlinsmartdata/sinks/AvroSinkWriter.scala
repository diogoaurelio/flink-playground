package com.berlinsmartdata.sinks


import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileConstants, DataFileWriter}

import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord

import org.apache.avro.specific.SpecificRecordBase

import scala.reflect.ClassTag
import org.apache.flink.streaming.connectors.fs.StreamWriterBase
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.FSDataOutputStream



/**
  * Scala custom Avro Writer for Flink
  */
class AvroSinkWriter[T <: SpecificRecordBase : ClassTag] extends StreamWriterBase[T] {

  val serialVersionUID = 1L

  @transient private var outputWriter: DataFileWriter[GenericRecord] = null
  //@transient private var outputWriter: DataFileWriter[T] = null
  //@transient private var outputWriter: GenericDatumWriter[T] = null
  @transient private var userDefinedSchema: Schema = null

  def setSchema(schema: Schema) { userDefinedSchema = schema }

  def getSchema(): Schema =
    if(userDefinedSchema != null)
      userDefinedSchema
    else
      extractSchema


  def extractSchema[T <: SpecificRecordBase : ClassTag]: Schema = {
    val mirror = scala.reflect.runtime.universe.runtimeMirror(getClass.getClassLoader)
    val moduleSymbol = mirror.staticModule(implicitly[ClassTag[T]].runtimeClass.getName)
    val instanceMirror = mirror.reflect(mirror.reflectModule(moduleSymbol).instance)

    val schemaVal = moduleSymbol.typeSignature.declarations
      .filter(_.asTerm.isVal)
      .filter(_.name.toString.contains("SCHEMA$"))
      .head

    instanceMirror.reflectField(schemaVal.asTerm).get.asInstanceOf[Schema]
  }

  override def open(fs: FileSystem, path: Path): Unit = {
    super.open(fs, path)
    setSchema(getSchema())

    if (userDefinedSchema == null)
      throw new IllegalStateException("Avro Schema required to create AvroSinkWriter")

    // TODO
    //val compressionCodec: CodecFactory = getCompressionCodec(properties)

    buildDatumWriter(getStream())

  }

  override def write(element: T) = {
    if (outputWriter == null)
      throw new IllegalStateException("AvroSinkWriter has not been opened")

    outputWriter.append(element)

  }

  override def close {
    if (outputWriter != null) {
      outputWriter.sync()
      outputWriter.close()
    }

    outputWriter = null
    super.close()
  }

  override def duplicate() = new AvroSinkWriter[T]()

  private def buildDatumWriter(outStream: FSDataOutputStream,
                               compressionCodec: Option[CodecFactory] = None,
                               syncInterval: Option[Int] = Some(DataFileConstants.DEFAULT_SYNC_INTERVAL)): Unit = {

    val writer = new GenericDatumWriter[GenericRecord](getSchema())
    val avroFileWriter = new DataFileWriter[GenericRecord](writer)


    if (compressionCodec.isDefined)
      avroFileWriter.setCodec(compressionCodec.get)
    if (syncInterval.isDefined)
      avroFileWriter.setSyncInterval(syncInterval.get)

    outputWriter = avroFileWriter
    outputWriter.create(userDefinedSchema, outStream)

  }

}

object AvroSinkWriter {

  lazy val CONF_OUTPUT_SCHEMA = "avro.schema.output"
  lazy val CONF_COMPRESS = FileOutputFormat.COMPRESS
  lazy val CONF_COMPRESS_CODEC = FileOutputFormat.COMPRESS_CODEC
  lazy val CONF_DEFLATE_LEVEL = "avro.deflate.level"
  lazy val CONF_XZ_LEVEL = "avro.xz.level"

}
