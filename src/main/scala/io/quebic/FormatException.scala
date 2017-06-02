package io.quebic

class FormatException(msg:String, ex:Exception) extends Exception(msg, ex) {
  def this(msg:String) = this(msg, null)
}
