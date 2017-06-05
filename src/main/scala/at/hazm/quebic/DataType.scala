package at.hazm.quebic

/**
  *
  * @param id 2bit 幅の識別子
  * @param name 型の名前
  */
sealed abstract class DataType(val id:Byte, val name:String) extends Type

object DataType {
  val values:Seq[DataType] = Seq(INTEGER, REAL, TEXT, BINARY, VECTOR, MATRIX, TENSOR)
  private[this] val valuesMap = values.groupBy(_.id).mapValues(_.head)

  values.foreach { x => (x.id & 0xFF) <= 0x0F }

  val BIT_WIDTH = 4
  val BIT_MASK = 0x0F
  val NUM_IN_BYTE = 8 / BIT_WIDTH

  def valueOf(id:Byte):DataType = valuesMap(id)

  case object INTEGER extends DataType(0, "int")

  case object REAL extends DataType(1, "real")

  case object TEXT extends DataType(2, "text")

  case object BINARY extends DataType(3, "binary")

  case object VECTOR extends DataType(4, "vector")

  case object MATRIX extends DataType(5, "matrix")

  case object TENSOR extends DataType(6, "tensor")

}
