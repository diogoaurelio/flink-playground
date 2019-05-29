package com.datenn.sinks

import org.apache.flink.streaming.connectors.fs.Clock
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer
import org.apache.hadoop.fs.Path
import org.joda.time.{DateTime, DateTimeZone}


trait EventTimeBucketer[T] extends Bucketer[T] {

  private var datePartitioning = "yyyy/MM/dd/HH/"

  def setDatePartitioning(part: String) { this.datePartitioning = part }

  override def getBucketPath(clock: Clock, basePath: Path, element: T): Path =
    new Path(basePath + "/" + getDateTime(element))

  def getDateTime(element: T) =
    new DateTime(getEventTime(element) * 1000L, DateTimeZone.UTC)
      .toDateTime
      .toString(datePartitioning)

  /**
    * Extracts from a given element the time field
    *
    * @param element
    */
  def getEventTime(element: T): Long

}
