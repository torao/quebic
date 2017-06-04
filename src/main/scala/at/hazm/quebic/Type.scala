package at.hazm.quebic

trait Type {
  val id:Byte
  val name:String

  override def toString:String = name
}
