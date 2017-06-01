package io.quebic

sealed abstract class CompressionType(val id:Byte, val name:String) extends Type

object CompressionType {
  val values:Seq[CompressionType] = Seq(NONE, GZIP)
  private[this] val valuesMap = values.groupBy(_.id).mapValues(_.head)

  def valueOf(id:Byte):CompressionType = valuesMap(id)

  case object NONE extends CompressionType(0, "none")

  case object GZIP extends CompressionType(1, "gzip")

}
