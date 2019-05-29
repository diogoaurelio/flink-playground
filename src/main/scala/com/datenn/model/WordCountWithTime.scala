package com.datenn.model

import org.joda.time.DateTime

case class WordCountWithTime(word: String, count: Int, time: Long, dt: DateTime) extends EventWithTime {

  override def getEventTime(): Long = time

  def +(anotherWord: WordCountWithTime) =
    WordCountWithTime(word, count + anotherWord.count, time, dt)
}
