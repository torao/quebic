package at.hazm.quebic

import java.nio.ByteBuffer

import org.specs2.Specification
import org.specs2.execute.Result
import org.specs2.specification.core.SpecStructure

import scala.util.Random

class SchemaTest extends Specification {
  override def is:SpecStructure =
    s2"""
struct serialization: $serialization
self serialization: $toByteArray
variable integer: $variableInteger
exception cases: $exceptionCases
"""

  def serialization:Result = {
    val random = new Random(55)
    val intValue = random.nextInt()
    val doubleValue = random.nextDouble()
    val textValue = random.nextString(1024)
    val binaryValue = new Array[Byte](1024)
    random.nextBytes(binaryValue)
    val s1 = Struct(Struct.INTEGER(intValue), Struct.REAL(doubleValue), Struct.TEXT(textValue), Struct.BINARY(binaryValue))

    val schema = Schema(DataType.INTEGER, DataType.REAL, DataType.TEXT, DataType.BINARY)
    Seq(Codec.PLAIN, Codec.GZIP).map { codec =>
      val buffer = schema.serialize(s1, codec)
      System.out.println(f"${codec.name.toUpperCase}: ${buffer.remaining()}%,d bytes")
      val s2 = schema.deserialize(buffer, codec)
      (s1.values.length === s2.values.length) and s2.values.zip(s1.values).map {
        case (Struct.INTEGER(a), Struct.INTEGER(b)) => a === b
        case (Struct.REAL(a), Struct.REAL(b)) => a === b
        case (Struct.TEXT(a), Struct.TEXT(b)) => a === b
        case (Struct.BINARY(a), Struct.BINARY(b)) => a === b
        case (a, b) => a === b
      }.reduceLeft(_ and _)
    }
  }

  def toByteArray:Result = {
    val schema1 = Schema(DataType.INTEGER, DataType.REAL, DataType.TEXT, DataType.BINARY)
    val binary = schema1.toByteArray
    val schema2 = Schema(ByteBuffer.wrap(binary))
    (schema1.types === schema2.types) and (schema1 === schema2)
  }

  def variableInteger:Result = {
    def test(num:Long):Result = {
      val schema = Schema(DataType.INTEGER)
      val struct1 = Struct(Struct.INTEGER(num))
      val struct2 = schema.deserialize(schema.serialize(struct1, Codec.PLAIN), Codec.PLAIN)
      (num === struct2.values.head.asInstanceOf[Struct.INTEGER].value) and (struct1 === struct2)
    }

    Seq[Long](0, 252, 253, 254, 255, 256, 0xFFFF, 0x10000, 0xFFFFFFFFL, 0x100000000L, Long.MaxValue).map{ num =>
      test(num) and test(- num)
    }.reduceLeft(_ and _)
  }

  def exceptionCases:Result = {
    val columnSizeOverflow = Schema((0 to Limits.MaxColumnSize).map(_ => DataType.INTEGER):_*) must throwA[FormatException]
    val columnSizeIncompatible = Schema(DataType.INTEGER).serialize(Struct(), Codec.PLAIN) must throwA[IncompatibleSchemaException]
    val columnTypeIncompatible = Schema(DataType.INTEGER).serialize(Struct(Struct.TEXT("1")), Codec.PLAIN) must throwA[IncompatibleSchemaException]
    columnSizeOverflow and columnSizeIncompatible and columnTypeIncompatible
  }

}
