package com.berlinsmartdata.testutils

import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


class TestCollector[T] extends Collector[T] {

  val list = ListBuffer.empty[T]

  override def collect(record: T): Unit = {
    list.+=(record)
  }

  def head = list.head

  override def close(): Unit = {}
}
