package at.hazm.quebic

import java.io.DataOutputStream

import at.hazm.quebic.Struct.Data

import scala.annotation.tailrec
import scala.collection.mutable

case class Struct(values:Data[_]*) {
  override def toString:String = values.mkString("[", ",", "]")
}

object Struct {

  sealed trait Data[T] {
    def value:T

    def dataType:DataType[T]

    def writeTo(out:DataOutputStream):Unit = dataType.write(value, out)
  }

  case class INTEGER(value:Long) extends Data[Long] {
    override def dataType:DataType[Long] = DataType.INTEGER

    override def toString:String = value.toString
  }

  case class REAL(value:Double) extends Data[Double] {
    override def dataType:DataType[Double] = DataType.REAL

    override def toString:String = value.toString
  }

  case class TEXT(value:String) extends Data[String] {
    override def dataType:DataType[String] = DataType.TEXT

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
    override def dataType:DataType[Array[Byte]] = DataType.BINARY

    override def toString:String = value.map(x => f"$x%02X").mkString("0x", "", "")
  }

  case class TENSOR(shape:Seq[Int], values:Seq[Double]) extends Data[(Seq[Int], Seq[Double])] {
    if(shape.product != values.length || shape.isEmpty && values.nonEmpty) {
      throw new IllegalArgumentException(s"invalid data length: ${shape.map(_.toString).mkString("x")} != ${values.length}")
    }

    val value:(Seq[Int], Seq[Double]) = (shape, values)

    override def dataType:DataType[(Seq[Int], Seq[Double])] = DataType.TENSOR

    override def toString:String = TENSOR.toString(shape, values)
  }

  object TENSOR {

    def fromVector(values:Any*):TENSOR = fromTensor(values)

    def fromMatrix(values:Seq[Any]*):TENSOR = fromTensor(values)

    def fromTensor(values:Seq[Any]):TENSOR = {
      @tailrec
      def _shape(values:Seq[Any], shape:mutable.Buffer[Int] = mutable.Buffer[Int]()):Seq[Int] = if(values.isEmpty) {
        throw new IllegalArgumentException(f"empty array detected in shape ${shape.mkString("[", ",", "]")}")
      } else {
        shape.append(values.length)
        values.head match {
          case array:Seq[Any] => _shape(array, shape)
          case (_:Boolean | _:Byte | _:Short | _:Int | _:Long | _:Float | _:Double) => shape
          case unexpected =>
            throw new IllegalArgumentException(f"unexpected scalar value detected in shape ${shape.mkString("[", ",", "]")}@0 = $unexpected")
        }
      }

      def _transform(values:Seq[Any], shape:Seq[Int], depth:Int = 0, pos:Seq[Int] = Seq.empty):Seq[Double] = {
        if(values.length != shape(depth)) {
          throw new IllegalArgumentException(f"dimension conflict in ${(pos :+ 0).mkString("[", ",", "]")}: ${values.length} != ${shape(depth)}")
        }
        if(depth == shape.length - 1) {
          values.zipWithIndex.map {
            case (x:Boolean, _) => if(x) 1.0 else 0.0
            case (x:Byte, _) => x.toDouble
            case (x:Short, _) => x.toDouble
            case (x:Int, _) => x.toDouble
            case (x:Long, _) => x.toDouble
            case (x:Float, _) => x.toDouble
            case (x:Double, _) => x
            case (unexpected, i) =>
              throw new IllegalArgumentException(f"unexpected scalar value detected in ${(pos :+ i).mkString("[", ",", "]")} = $unexpected")
          }
        } else {
          values.zipWithIndex.flatMap {
            case (array:Seq[Any], i) => _transform(array, shape, depth + 1, pos :+ i)
            case (unexpected, i) =>
              throw new IllegalArgumentException(f"expect array but some value detected in ${(pos :+ i).mkString("[", ",", "]")}: $unexpected; shape=${shape.mkString("[", ",", "]")}")
          }
        }
      }

      val shape = _shape(values)
      val x = _transform(values, shape)
      TENSOR(shape.toArray, x.toArray)
    }

    private[Struct] def toString(shape:Seq[Int], values:Seq[Double]):String = {
      def _tos(f:Double):String = if(f == math.floor(f)) f"$f%.0f" else f.toString

      def _toString(i:Int):Iterator[String] = if(i == shape.length - 1) {
        values.grouped(shape(i)).map(_.map(_tos).mkString("[", ",", "]"))
      } else {
        _toString(i + 1).grouped(shape(i)).map(_.mkString("[", ",", "]"))
      }

      _toString(0).next()
    }
  }

}