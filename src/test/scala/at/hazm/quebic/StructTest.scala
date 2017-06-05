package at.hazm.quebic

import org.specs2.Specification
import org.specs2.execute.Result
import org.specs2.specification.core.SpecStructure

class StructTest extends Specification {
  override def is:SpecStructure =
    s2"""
data and type to string: $dataToString
"""

  def dataToString:Result = {
    Seq(
      Struct.INTEGER(0),
      Struct.REAL(0.0),
      Struct.TEXT((0 to 0xFF).map(_.toChar).toString()),
      Struct.BINARY((0 to 0xFF).map(_.toByte).toArray)
    ).map { data =>
      data.dataType
      data.toString
      success
    }
    success
  }

}
