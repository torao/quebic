package at.hazm.quebic

import java.io.{DataInputStream, DataOutputStream}
import java.nio.charset.StandardCharsets

/**
  *
  * @param id   2bit 幅の識別子
  * @param name 型の名前
  */
sealed abstract class DataType[T](val id:Byte, val name:String) extends Type {
  def write(struct:T, out:DataOutputStream):Unit

  def read(in:DataInputStream):T
}

object DataType {
  val values:Seq[DataType[_]] = Seq(INTEGER, REAL, TEXT, BINARY, TENSOR)
  private[this] val valuesMap = values.groupBy(_.id).mapValues(_.head)

  values.foreach { x => (x.id & 0xFF) <= 0x0F }

  val BIT_WIDTH:Int = 4
  val BIT_MASK:Int = 0x0F
  val NUM_IN_BYTE:Int = 8 / BIT_WIDTH

  def valueOf(id:Byte):DataType[_] = valuesMap(id)

  case object INTEGER extends DataType[Long](0, "int") {
    def write(value:Long, out:DataOutputStream):Unit = writeLong(value, out)

    def read(in:DataInputStream):Long = readLong(in)
  }

  case object REAL extends DataType[Double](1, "real") {
    def write(value:Double, out:DataOutputStream):Unit = out.writeDouble(value)

    def read(in:DataInputStream):Double = in.readDouble()
  }

  case object TEXT extends DataType[String](2, "text") {
    def write(value:String, out:DataOutputStream):Unit = writeText(value, out)

    def read(in:DataInputStream):String = readText(in)
  }

  case object BINARY extends DataType[Array[Byte]](3, "binary") {
    def write(value:Array[Byte], out:DataOutputStream):Unit = writeBinary(value, out)

    def read(in:DataInputStream):Array[Byte] = readBinary(in)
  }

  case object TENSOR extends DataType[(Seq[Int], Seq[Double])](4, "tensor") {
    def write(value:(Seq[Int], Seq[Double]), out:DataOutputStream):Unit = {
      writeLong(value._1.length, out)
      value._1.foreach { i => writeLong(i, out) }
      value._2.foreach { f => out.writeDouble(f) }
    }

    def read(in:DataInputStream):(Seq[Int], Seq[Double]) = {
      val length = readLong(in).toInt
      val shape = (0 until length).map { _ => readLong(in).toInt }
      val values = (0 until shape.product).map { _ => in.readDouble() }
      (shape.toArray, values.toArray)
    }
  }

  private[DataType] def writeText(value:String, out:DataOutputStream):Unit = {
    val bin = value.getBytes(StandardCharsets.UTF_8)
    writeBinary(bin, out)
  }

  private[DataType] def readText(in:DataInputStream):String = new String(readBinary(in), StandardCharsets.UTF_8)

  private[DataType] def writeBinary(value:Array[Byte], out:DataOutputStream):Unit = {
    writeLong(value.length, out)
    out.write(value)
  }

  private[DataType] def readBinary(in:DataInputStream):Array[Byte] = {
    val size = readLong(in).toInt
    val binary = new Array[Byte](size)
    in.readFully(binary)
    binary
  }

  private[DataType] def writeLong(value:Long, out:DataOutputStream):Unit = if(value >= 0) {
    // bitcoin 方式
    // 0～252 なら整数そのもの、253 なら続く 2 バイトに値が入っている、254 なら続く 4 バイトに値が入っている、255 なら続く 8 バイトに値が入っている
    value match {
      case b if b <= 252 =>
        out.write(b.toByte)
      case s if s <= 0xFFFF =>
        out.write(253.toByte)
        out.writeShort(s.toShort)
      case i if i <= 0xFFFFFFFFL =>
        out.write(254.toByte)
        out.writeInt(i.toInt)
      case l =>
        out.write(255.toByte)
        out.writeLong(l)
    }
  } else {
    out.write(255.toByte)
    out.writeLong(value)
  }

  private[DataType] def readLong(in:DataInputStream):Long = in.readByte() & 0xFF match {
    case b if b <= 252 => b
    case s if s == 253 => in.readShort() & 0xFFFF
    case i if i == 254 => in.readInt() & 0xFFFFFFFFL
    case _ => in.readLong()
  }

}
