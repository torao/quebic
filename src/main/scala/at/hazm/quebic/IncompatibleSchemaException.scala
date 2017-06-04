package at.hazm.quebic

class IncompatibleSchemaException(msg:String, ex:Exception) extends FormatException(msg, ex) {
  def this(msg:String) = this(msg, null)
}
