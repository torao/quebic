package io.quebic

import io.quebic.Struct.Data

case class Struct(values:Data[_]*)

object Struct {

  sealed trait Data[T] {
    def value:T
  }

  case class INTEGER(value:Long) extends Data[Long]

  case class REAL(value:Double) extends Data[Double]

  case class TEXT(value:String) extends Data[String]

  case class BINARY(value:Array[Byte]) extends Data[Array[Byte]]

}