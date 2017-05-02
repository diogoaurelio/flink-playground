package com.berlinsmartdata.model

import org.apache.avro.Schema
import org.joda.time.DateTime
import org.apache.avro.specific.SpecificRecordBase

import scala.annotation.switch


case class WordCountWithTimeAvroFormat(var word: String, var count: Int, var time: Long, var dt: String) extends SpecificRecordBase with EventWithTime {

  override def getEventTime(): Long = time

  def +(anotherWord: WordCountWithTime) =
    WordCountWithTimeAvroFormat(word, count + anotherWord.count, time, dt)

  override def get(field: Int): AnyRef =  {
    (field: @switch) match {
      case pos if pos == 0 => {
        word
      }.asInstanceOf[AnyRef]
      case pos if pos == 1 => {
        count
      }.asInstanceOf[AnyRef]
      case pos if pos == 2 => {
        time
      }.asInstanceOf[AnyRef]
      case pos if pos == 3 => {
        dt
      }
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
  }

  override def getSchema: Schema = WordCountWithTimeAvroFormat.SCHEMA$

  override def put(field: Int, value: scala.Any): Unit = {
    (field: @switch) match {
      case pos if pos == 0 => this.word = {
        value
      }.asInstanceOf[String]
      case pos if pos == 1 => this.count = {
        value
      }.asInstanceOf[Int]
      case pos if pos == 2 => this.time = {
        value
      }.asInstanceOf[Long]
      case pos if pos == 3 => this.dt = {
        value
      }.asInstanceOf[String]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
    ()
  }
}

object WordCountWithTimeAvroFormat {
  val SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"WordCountWithTimeAvroFormat\",\"namespace\":\"com.berlinsmartdata.model\",\"fields\":[{\"name\":\"word\",\"type\":\"string\"},{\"name\":\"count\",\"type\":\"int\"},{\"name\":\"time\",\"type\":\"long\"},{\"name\":\"dt\",\"type\":\"string\"}]}")
}
