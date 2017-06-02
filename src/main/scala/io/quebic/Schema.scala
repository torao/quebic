package io.quebic

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.zip.GZIPOutputStream

import org.slf4j.LoggerFactory

case class Schema(types:DataType*) {
  if(types.length > Limits.MaxColumnSize) {
    throw new FormatException(f"too many schema types: ${types.length},d > ${Limits.MaxColumnSize},d")
  }

  /**
    * 指定されたフィールドをこのスキーマで定義される形式でシリアライズします。スキーマとフィールドセットに互換性がない
    * 場合は例外がスローされます。
    *
    * @param struct      シリアライズするフィールドセット
    * @param compression フィールドの圧縮形式
    * @return フィールドのバッファ
    */
  def serialize(struct:Struct, compression:CompressionType):ByteBuffer = {
    if(struct.values.length != types.length) {
      throw new IllegalArgumentException(s"struct values are incompatible for schema ${struct.values.mkString.length}")
    }
    val baos = new ByteArrayOutputStream()
    val os = compression match {
      case CompressionType.NONE => baos
      case CompressionType.GZIP => new GZIPOutputStream(baos)
    }
    val out = new DataOutputStream(baos)
    struct.values.zip(types).foreach {
      case (Struct.INTEGER(value), DataType.INTEGER) =>
        Schema.writeLong(value, out)
      case (Struct.REAL(value), DataType.REAL) =>
        out.writeDouble(value)
      case (Struct.TEXT(value), DataType.TEXT) =>
        val bin = value.getBytes(StandardCharsets.UTF_8)
        Schema.writeBinary(bin, out)
      case (Struct.BINARY(value), DataType.BINARY) =>
        Schema.writeBinary(value, out)
      case (value, t) =>
        throw new IllegalArgumentException(s"incompatible struct field type: expect ${t.name}, actual ${Schema.escape(value.value)}")
    }
    out.close()
    ByteBuffer.wrap(baos.toByteArray)
  }

  /**
    * 指定されたバッファからこのスキーマで定義されるフィールドを読み込みます。
    *
    * @param buffer フィールドを読み込むバッファ
    * @return 読み込んだフィールド
    */
  def deserialize(buffer:ByteBuffer):Struct = Struct(
    types.map {
      case DataType.INTEGER =>
        Struct.INTEGER(Schema.readLong(buffer))
      case DataType.REAL =>
        Struct.REAL(buffer.getDouble)
      case DataType.TEXT =>
        Struct.TEXT(new String(Schema.readBinary(buffer), StandardCharsets.UTF_8))
      case DataType.BINARY =>
        Struct.BINARY(Schema.readBinary(buffer))
    }:_*
  )

  /**
    * このスキーマをバイナリに変換します。
    *
    * @return スキーマのバイナリ
    */
  def toByteArray:Array[Byte] = (types.length.toByte +: types.map(_.id).grouped(2).map { ids =>
    (((ids.head & 0x03) << 4) | (if(ids.size == 1) 0 else ids(1) & 0x03)).toByte
  }.toSeq).toArray

  override def toString:String = types.map(_.name).mkString("[", ",", "]")

}

object Schema {
  private[Schema] val logger = LoggerFactory.getLogger(classOf[Schema])

  /**
    * 指定されたバッファの現在の位置からスキーマを読み込んで復元します。
    *
    * @param buf スキーマを復元するバイナリ
    * @return スキーマ
    */
  def apply(buf:ByteBuffer):Schema = {
    val count = buf.get() & 0xFF
    val byteSize = (count >> 1) + (count & 0x01)
    Schema((0 until byteSize).flatMap { i =>
      val ts = buf.get() & 0xFF
      Seq(((ts >> 4) & 0x03).toByte, (ts & 0x03).toByte)
    }.take(count).map(DataType.valueOf):_*)
  }

  private[Schema] def writeBinary(value:Array[Byte], out:DataOutputStream):Unit = {
    writeLong(value.length, out)
    out.write(value)
  }

  private[Schema] def readBinary(buffer:ByteBuffer):Array[Byte] = {
    val size = readLong(buffer).toInt
    val binary = new Array[Byte](size)
    buffer.get(binary)
    binary
  }

  private[Schema] def writeLong(value:Long, out:DataOutputStream):Unit = if(value >= 0) {
    // bitcoin 方式
    // 0～252 なら整数そのもの、253 なら続く 2 バイトに値が入っている、254 なら続く 4 バイトに値が入っている、255 なら続く 8 バイトに値が入っている
    value match {
      case b if b <= 252 =>
        out.write(value.toByte)
      case s if s <= 0xFFFF =>
        out.write(253.toByte)
        out.writeShort(value.toShort)
      case i if i <= 0xFFFFFFFFL =>
        out.write(254.toByte)
        out.writeInt(value.toInt)
      case l =>
        out.write(255.toByte)
        out.writeLong(value)
    }
  } else {
    out.write(255.toByte)
    out.writeLong(value)
  }

  private[Schema] def readLong(buffer:ByteBuffer):Long = buffer.get() & 0xFF match {
    case b if b <= 252 => b
    case s if s == 253 => buffer.getShort & 0xFFFF
    case i if i == 254 => buffer.getInt & 0xFFFFFFFFL
    case _ => buffer.getLong
  }

  private[Schema] def escape(value:Any):String = value match {
    case null => "null"
    case str:String =>
      "\"" + (if(str.length > 25) str.substring(0, 25) + "..." else str).replaceAll("\"", "\"\"") + "\""
    case arr:Array[_] if arr.getClass.getComponentType == classOf[Byte] =>
      (if(arr.length > 25) arr.take(25) else arr).map(_.asInstanceOf[Byte]).map(b => f"${b & 0xFF}%02X").mkString + (if(arr.length > 25) "..." else "")
    case arr:Array[_] => arr.map(escape).mkString("[", ",", "]")
    case arr:Iterable[_] => arr.map(escape).mkString("[", ",", "]")
    case x => x.toString
  }

}