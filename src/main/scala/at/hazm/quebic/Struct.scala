package at.hazm.quebic

import at.hazm.quebic.Struct.Data

case class Struct(values:Data[_]*) {
  override def toString:String = values.mkString("[", ",", "]")
}

object Struct {

  sealed trait Data[T] {
    def typeName:String

    def value:T
  }

  case class INTEGER(value:Long) extends Data[Long] {
    override def typeName:String = "int"

    override def toString:String = value.toString
  }

  case class REAL(value:Double) extends Data[Double] {
    override def typeName:String = "real"

    override def toString:String = value.toString
  }

  case class TEXT(value:String) extends Data[String] {
    override def typeName:String = "text"

    override def toString:String = value.map {
      case '\b' => "\\b"
      case '\f' => "\\f"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case '\t' => "\\t"
      case '\"' => "\\\""
      case '\'' => "\\\'"
      case '\\' => "\\\\"
      case ch if !Character.isDefined(ch) || Character.isISOControl(ch) => f"\\u${ch.toInt}%04X"
      case ch => ch.toString
    }.mkString("\"", "", "\"")
  }

  case class BINARY(value:Array[Byte]) extends Data[Array[Byte]] {
    override def typeName:String = "binary"

    override def toString:String = value.map(x => f"$x%02X").mkString("0x", "", "")
  }

}