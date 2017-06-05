package at.hazm.quebic

import java.io._
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import org.slf4j.LoggerFactory

case class Schema(types:DataType*) {
  if(types.length > Limits.MaxColumnSize) {
    throw new FormatException(f"too many schema types: ${types.length},d > ${Limits.MaxColumnSize},d")
  }

  /**
    * 指定されたフィールドをこのスキーマで定義される形式でシリアライズします。スキーマとフィールドセットに互換性がない
    * 場合は例外がスローされます。
    *
    * @param struct シリアライズするフィールドセット
    * @param codec  フィールドの圧縮形式
    * @return フィールドのバッファ
    */
  def serialize(struct:Struct, codec:Codec):ByteBuffer = {
    if(struct.values.length != types.length) {
      throw new IncompatibleSchemaException(s"struct values are incompatible for schema ${struct.values.mkString.length}")
    }
    val baos = new ByteArrayOutputStream()
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
      case (Struct.VECTOR(values), DataType.VECTOR) =>
        Schema.writeLong(values.length, out)
        values.foreach{ value => out.writeDouble(value) }
      case (Struct.TENSOR(shape, values), DataType.TENSOR) =>
        Schema.writeLong(shape.length, out)
        shape.foreach{ value => Schema.writeLong(value, out) }

      case (value, t) =>
        throw new IncompatibleSchemaException(s"incompatible struct field type: expect ${t.name}, actual ${value.dataType.name} ($value)")
    }
    out.close()
    ByteBuffer.wrap(codec.encode(baos.toByteArray))
  }

  /**
    * 指定されたバッファからこのスキーマで定義されるフィールドを読み込みます。
    *
    * @param buffer フィールドを読み込むバッファ
    * @return 読み込んだフィールド
    */
  def deserialize(buffer:ByteBuffer, codec:Codec):Struct = {

    val array = new Array[Byte](buffer.remaining())
    buffer.get(array)
    val in = new DataInputStream(new ByteArrayInputStream(codec.decode(array)))

    Struct(
      types.map {
        case DataType.INTEGER =>
          Struct.INTEGER(Schema.readLong(in))
        case DataType.REAL =>
          Struct.REAL(in.readDouble())
        case DataType.TEXT =>
          Struct.TEXT(new String(Schema.readBinary(in), StandardCharsets.UTF_8))
        case DataType.BINARY =>
          Struct.BINARY(Schema.readBinary(in))
        case DataType.VECTOR =>
          Struct.VECTOR
      }:_*
    )
  }

  /**
    * このスキーマをバイナリに変換します。
    *
    * @return スキーマのバイナリ
    */
  def toByteArray:Array[Byte] = {
    val paddingLength = Schema.bit4ToBit8AlignSize(types.length)
    (types.length.toByte +: types.map(_.id).padTo(paddingLength, 0.toByte).grouped(4).map { i =>
      i.indices.map{ j =>
        (i(j) & DataType.BIT_MASK) << (8 - ((j + 1) * DataType.BIT_WIDTH))
      }.reduceLeft(_ | _).toByte
    }.toSeq).toArray
  }

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
    val byteSize = bit4ToBit8AlignSize(count) / DataType.NUM_IN_BYTE
    Schema((0 until byteSize).flatMap { _ =>
      val ts = buf.get() & 0xFF
      (0 until DataType.NUM_IN_BYTE).map { i =>
        ((ts >> (8 - ((i + 1) * DataType.BIT_WIDTH))) & DataType.BIT_MASK).toByte
      }
    }.take(count).map(DataType.valueOf):_*)
  }

  private[Schema] def bit4ToBit8AlignSize(len:Int):Int = len + (if(len % DataType.NUM_IN_BYTE == 0) 0 else DataType.NUM_IN_BYTE - len % 2)

  private[Schema] def writeBinary(value:Array[Byte], out:DataOutputStream):Unit = {
    writeLong(value.length, out)
    out.write(value)
  }

  private[Schema] def readBinary(in:DataInputStream):Array[Byte] = {
    val size = readLong(in).toInt
    val binary = new Array[Byte](size)
    in.readFully(binary)
    binary
  }

  private[Schema] def writeLong(value:Long, out:DataOutputStream):Unit = if(value >= 0) {
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

  private[Schema] def readLong(in:DataInputStream):Long = in.readByte() & 0xFF match {
    case b if b <= 252 => b
    case s if s == 253 => in.readShort() & 0xFFFF
    case i if i == 254 => in.readInt() & 0xFFFFFFFFL
    case _ => in.readLong()
  }

}