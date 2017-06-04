package at.hazm.quebic

/**
  *
  * @param id 2bit 幅の識別子
  * @param name 型の名前
  */
sealed abstract class DataType(val id:Byte, val name:String) extends Type

object DataType {
  val values:Seq[DataType] = Seq(INTEGER, REAL, TEXT, BINARY)
  private[this] val valuesMap = values.groupBy(_.id).mapValues(_.head)

  values.foreach { x => (x.id & 0xFF) < 0x04 }

  def valueOf(id:Byte):DataType = valuesMap(id)

  case object INTEGER extends DataType(0, "int")

  case object REAL extends DataType(1, "real")

  case object TEXT extends DataType(2, "text")

  case object BINARY extends DataType(3, "binary")

}
