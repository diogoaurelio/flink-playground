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


/**
  * Custom Avro Writer for Flink
  */
class AvroSinkWriter[T <: SpecificRecordBase : ClassTag] extends StreamWriterBase[T] {

  val serialVersionUID = 1L

  @transient private var outputWriter: Option[DataFileWriter[GenericRecord]] = None

  private var userDefinedSchema: Option[Schema] = None
  private var compressionCodec: Option[CodecFactory] = None
  private var syncInterval: Option[Int] = None


  override def open(fs: FileSystem, path: Path): Unit = {
    super.open(fs, path)
    setSchema(getSchema)

    if (userDefinedSchema.isEmpty)
      throw new IllegalStateException("Avro Schema required to create AvroSinkWriter")


    val writer = new GenericDatumWriter[GenericRecord](getSchema)
    val avroFileWriter = new DataFileWriter[GenericRecord](writer)

    if (compressionCodec.isDefined)
      avroFileWriter.setCodec(compressionCodec.get)
    if (syncInterval.isDefined)
      avroFileWriter.setSyncInterval(syncInterval.get)

    outputWriter = Some(avroFileWriter)
    outputWriter.get.create(getSchema, getStream)

  }

  override def write(element: T) = {
    if (outputWriter.isEmpty)
      throw new IllegalStateException("AvroSinkWriter has not been opened")
    outputWriter.get.append(element)
  }

  override def close {
    if (outputWriter != null) {
      outputWriter.get.sync()
      outputWriter.get.close()
    }
    outputWriter = None
    super.close()
  }

  override def duplicate() = new AvroSinkWriter[T]()

  def setSchema(schema: Schema) { userDefinedSchema = Some(schema) }

  def getSchema: Schema = userDefinedSchema.getOrElse(extractSchema)

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

  private def getBoolean(conf: Map[String,String], key: String, default: Boolean): Boolean =
    scala.util.Try(conf.getOrElse(key, "").toBoolean).getOrElse(default)

  private def getInt(conf: Map[String, String], key: String, default: Int): Int =
    scala.util.Try(conf.getOrElse(key, "").toInt).getOrElse(default)


  def setCompressionCodec(conf: Map[String,String]): Unit = {
    if (getBoolean(conf, AvroSinkWriter.CONF_COMPRESS, false)) {
      val deflateLevel = getInt(conf, AvroSinkWriter.CONF_DEFLATE_LEVEL, CodecFactory.DEFAULT_DEFLATE_LEVEL)
      val xzLevel = getInt(conf, AvroSinkWriter.CONF_XZ_LEVEL, CodecFactory.DEFAULT_XZ_LEVEL)
      val outputCodec = conf.getOrElse(AvroSinkWriter.CONF_COMPRESS_CODEC, "")

      if (DataFileConstants.DEFLATE_CODEC.equals(outputCodec)) {
        compressionCodec = Some(CodecFactory.deflateCodec(deflateLevel))
      } else if (DataFileConstants.XZ_CODEC.equals(outputCodec)) {
        compressionCodec = Some(CodecFactory.xzCodec(xzLevel))
      } else {
        compressionCodec = Some(CodecFactory.fromString(outputCodec))
      }
    }
    compressionCodec = Some(CodecFactory.nullCodec())
  }

  def setSyncInterval(sync: Option[Int] = None): Unit = {
    syncInterval = Some(sync.getOrElse(DataFileConstants.DEFAULT_SYNC_INTERVAL))
  }
}

object AvroSinkWriter {
  lazy val CONF_COMPRESS = FileOutputFormat.COMPRESS
  lazy val CONF_COMPRESS_CODEC = FileOutputFormat.COMPRESS_CODEC
  lazy val CONF_DEFLATE_LEVEL = "avro.deflate.level"
  lazy val CONF_XZ_LEVEL = "avro.xz.level"
}
