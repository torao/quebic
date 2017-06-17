package at.hazm.quebic

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.specs2.Specification
import org.specs2.execute.Result
import org.specs2.specification.core.SpecStructure

class StructTest extends Specification {
  override def is:SpecStructure =
    s2"""
data and type to string: $dataToString
serialize: $serialize
tensor data test: $tensor
"""

  def dataToString:Result = {
    Seq(
      Struct.INTEGER(0),
      Struct.REAL(0.0),
      Struct.TEXT((0 to 0xFF).map(_.toChar).toString()),
      Struct.BINARY((0 to 0xFF).map(_.toByte).toArray),
      Struct.TENSOR.fromTensor(Seq(Seq(1)))
    ).map { data =>
      data.dataType
      data.toString
      success
    }
    success
  }

  def serialize:Result = Seq(
    Struct.INTEGER(0),
    Struct.REAL(0.0),
    Struct.TEXT((0 to 0xFF).map(_.toChar).toString()),
    Struct.BINARY((0 to 0xFF).map(_.toByte).toArray),
    Struct.TENSOR.fromTensor(Seq(Seq(1)))
  ).map { data =>
    val schema = Schema(data.dataType)
    val other = schema.deserialize(schema.serialize(Struct(data), Codec.PLAIN), Codec.PLAIN).values.head

    data.value === other.value
  }.reduceLeft(_ and _)

  def tensor:Result = {

    val vector = Struct.TENSOR.fromVector(0, 1, 2, 3)
    System.out.println(s"Vector: $vector")
    val verifyVector = (vector.shape.length === 1) and (vector.shape.head === 4) and (vector.values.length === 4)

    val matrix = Struct.TENSOR.fromMatrix(Seq(0.0, 1.0, 2.0), Seq(10.0, 11.0, 12.0))
    System.out.println(s"Matrix: $matrix")
    val matrixVector = (matrix.shape.length === 2) and (matrix.shape.head === 2) and (matrix.shape(1) === 3) and (matrix.values.length === 6)

    val dimensionConflict = Struct.TENSOR(Seq(), Seq(0.0)) must throwA[IllegalArgumentException]

    val x = Struct.TENSOR.fromVector(0.toByte, 1.toShort, 2, 3.toLong, Float.NaN, Double.NaN, true, false)
    val implicitType = x.values.zip(Seq(0.0, 1.0, 2.0, 3.0, Double.NaN, Double.NaN, 1.0, 0.0)).map { case (a, b) =>
      if(b.isNaN) a.isNaN must beTrue else a === b
    }.reduceLeft(_ and _)

    val unexpectedScalar = Struct.TENSOR.fromVector(None) must throwA[IllegalArgumentException]
    val emptyArrayDetect = Struct.TENSOR.fromTensor(Seq()) must throwA[IllegalArgumentException]
    val mixScalaAndArray1 = Struct.TENSOR.fromVector(0.0, Seq(1, 2, 3)) must throwA[IllegalArgumentException]
    val mixScalaAndArray2 = Struct.TENSOR.fromVector(Seq(1, 2, 3), 0.0) must throwA[IllegalArgumentException]
    val mixDifferentSize = Struct.TENSOR.fromVector(Seq(0, 1, 2), Seq(3, 4)) must throwA[IllegalArgumentException]

    dimensionConflict and verifyVector and matrixVector and implicitType and unexpectedScalar and emptyArrayDetect and mixScalaAndArray1 and mixScalaAndArray2 and mixDifferentSize
  }

}
