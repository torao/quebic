package at.hazm.quebic

import at.hazm.quebic.Struct.Data

import scala.collection.mutable

case class Struct(values:Data[_]*) {
  override def toString:String = values.mkString("[", ",", "]")
}

object Struct {

  sealed trait Data {
    def dataType:DataType
  }

  case class INTEGER(value:Long) extends Data {
    override def dataType:DataType = DataType.INTEGER

    override def toString:String = value.toString
  }

  case class REAL(value:Double) extends Data {
    override def dataType:DataType = DataType.REAL

    override def toString:String = value.toString
  }

  case class TEXT(value:String) extends Data {
    override def dataType:DataType = DataType.TEXT

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

  case class BINARY(value:Array[Byte]) extends Data {
    override def dataType:DataType = DataType.BINARY

    override def toString:String = value.map(x => f"$x%02X").mkString("0x", "", "")
  }

  case class VECTOR(values:Array[Double]) extends Data {
    override def dataType:DataType = DataType.VECTOR

    override def toString:String = values.map(_.toString).mkString("[", ",", "]")
  }

  case class TENSOR(shape:Array[Int], values:Array[Double]) extends Data {
    if(shape.product != values.length){
      throw new IllegalArgumentException(s"invalid data length: ${shape.map(_.toString).mkString("x")} != ${values.length}")
    }
    override def dataType:DataType = DataType.TENSOR

    override def toString:String = values.map(_.toString).mkString("[", ",", "]")
  }

  object TENSOR {
    def fromScaler(value:Double):TENSOR = TENSOR(Array(1), Array(value))
    def fromVector(values:Seq[Double]):TENSOR = TENSOR(Array(values.length), values.toArray)
    def fromMatrix()
    def fromTensor(values:Array[Any]):TENSOR = {
      val shape = mutable.Buffer[Int]()
      values.head match {
        case value:Double =>
          shape.append(values.length)
        case values:Array[_] =>
      }
    }
  }

}