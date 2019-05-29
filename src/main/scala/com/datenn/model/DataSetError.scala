package com.datenn.model

case class DataSetError(offendingData: String, message: String) extends Exception(message)

