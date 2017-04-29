package com.berlinsmartdata.sinks

import com.berlinsmartdata.model.{EventWithTime, WordCountWithTime}
import org.apache.flink.streaming.connectors.fs.Clock
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer
import org.apache.hadoop.fs.Path
import org.joda.time.{DateTime, DateTimeZone}

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
import ru._

/**
  * Bucketer that assumes that case class events have their own
  * method definition of getEventTime
  * Note: not the best choice of implementation, left just for
  *       demonstration purposes
  */

class WordCountTimeBucketer[T <: EventWithTime] extends EventTimeBucketer[T] {

  /**
    * Note: issue with this implementation is that
    *       it requires that elements are of subtype
    *       EventWithTime, so that instances have
    *       {@code .getEventTime()}
    * @param element
    * @return
    */
  def getEventTime(element: T): Long = element.getEventTime()

}


/**
  * However, more scalable and less intrusive approach
  * would be to get per case class the time param
  * One way to do it is via reflection
  */
class WordCountTimeBucketerViaReflection[T: TypeTag : ClassTag] extends EventTimeBucketer[T] {

  def getEventTime(element: T): Long = {
    val mirror = scala.reflect.runtime.universe.runtimeMirror(getClass.getClassLoader)
    val im = mirror.reflect(element)
    typeOf[T].members.collect {
      case m: MethodSymbol if m.isCaseAccessor =>
        val name  = m.name.toString
        val value = im.reflectMethod(m).apply()
        if (name == "time")
          value
        else
          None
    }
      .filter(p => !p.isInstanceOf[Option[Any]])
      .head
      .asInstanceOf[Long]
  }

}
