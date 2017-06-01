package io.quebic

sealed abstract class DataType(val id:Byte, val name:String) extends Type

object DataType {
  val values:Seq[DataType] = Seq(INTEGER, REAL, TEXT, BINARY)
  private[this] val valuesMap = values.groupBy(_.id).mapValues(_.head)

  def valueOf(id:Byte):DataType = valuesMap(id)

  case object INTEGER extends DataType(1, "int")

  case object REAL extends DataType(2, "real")

  case object TEXT extends DataType(3, "text")

  case object BINARY extends DataType(4, "binary")

}
