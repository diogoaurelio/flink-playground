package com.berlinsmartdata.model


case class WordCount(word: String, count: Int) {
  def +(anotherWord: WordCount) =
    WordCount(word, count + anotherWord.count)
}
