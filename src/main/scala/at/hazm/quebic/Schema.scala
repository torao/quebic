package at.hazm.quebic

import java.io._
import java.nio.ByteBuffer

import org.slf4j.LoggerFactory

case class Schema(types:DataType[_]*) {
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
      case (data, dataType) if data.dataType == dataType => data.writeTo(out)
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
        case DataType.INTEGER => Struct.INTEGER(DataType.INTEGER.read(in))
        case DataType.REAL => Struct.REAL(DataType.REAL.read(in))
        case DataType.TEXT => Struct.TEXT(DataType.TEXT.read(in))
        case DataType.BINARY => Struct.BINARY(DataType.BINARY.read(in))
        case DataType.TENSOR =>
          val (shape, values) = DataType.TENSOR.read(in)
          Struct.TENSOR(shape, values)
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
    (types.length.toByte +: types.map(_.id).padTo(paddingLength, 0.toByte).grouped(DataType.NUM_IN_BYTE).map { i =>
      i.indices.map { j =>
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

}