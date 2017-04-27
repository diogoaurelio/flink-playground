package com.berlinsmartdata.sinks


import com.berlinsmartdata.model.EventWithTime
import org.apache.flink.streaming.connectors.fs.Clock
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer
import org.apache.hadoop.fs.Path
import org.joda.time.{DateTime, DateTimeZone}


class EventTimeBucketer[T <: EventWithTime] extends Bucketer[T] {

  override def getBucketPath(clock: Clock, basePath: Path, element: T): Path =
    new Path(basePath + "/" + getDateTime(element))

  private def getDateTime(event: T) =
    new DateTime(event.getEventTime() * 1000L, DateTimeZone.UTC)
      .toDateTime
      .toString("yyyy/MM/dd/hh/")

}
