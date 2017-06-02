package io.quebic

import java.io.DataOutputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

sealed abstract class DataType2(val id:Byte, val name:String) extends Type {

  /**
    * このデータタイプでシリアライズ可能な Java/Scala 型を参照します。
    *
    * @return シリアライズ可能な型
    */
  val compatibleTypes:Set[Class[_]]

  /**
    * 指定された出力ストリームにこのデータタイプが定義する形式で値を出力します。
    *
    * @param value 出力する値
    * @param out   出力先のバッファ
    */
  def write(value:Any, out:DataOutputStream):Unit

  /**
    * 指定されたバッファからこのデータタイプが定義する形式で値を取得します。
    *
    * @param buffer 読み込むバッファ
    * @return 読み込んだ値
    */
  def read[T](clazz:Class[T], buffer:ByteBuffer):T
}

object DataType2 {
  val values:Seq[DataType2] = Seq(INTEGER, REAL, TEXT, BINARY)
  private[this] val valuesMap = values.groupBy(_.id).mapValues(_.head)

  def valueOf(id:Byte):DataType2 = valuesMap(id)

  case object INTEGER extends DataType2(1, "int") {

    /**
      * このデータタイプでシリアライズ可能な Java/Scala 型を参照します。
      *
      * @return シリアライズ可能な型
      */
    override val compatibleTypes:Set[Class[_]] = LongToAny.keySet

    /**
      * 指定されたバッファに対してこのデータタイプが定義する形式で値を出力します。
      *
      * @param value 出力する値
      * @param out   出力先のバッファ
      */
    override def write(value:Any, out:DataOutputStream):Unit = {
      val longValue = AnyToLong(value)
      if(longValue >= 0) {
        // bitcoin 方式
        // 0～252 なら整数そのもの、253 なら続く 2 バイトに値が入っている、254 なら続く 4 バイトに値が入っている、255 なら続く 8 バイトに値が入っている
        longValue match {
          case b if b <= 252 =>
            out.write(longValue.toByte)
          case s if s <= 0xFFFF =>
            out.write(253.toByte)
            out.writeShort(longValue.toShort)
          case i if i <= 0xFFFFFFFFL =>
            out.write(254.toByte)
            out.writeInt(longValue.toInt)
          case l =>
            out.write(255.toByte)
            out.writeLong(longValue)
        }
      } else {
        out.write(255.toByte)
        out.writeLong(longValue)
      }
    }

    /**
      * 指定されたバッファからこのデータタイプが定義する形式で値を取得します。
      *
      * @param buffer 読み込むバッファ
      * @return 読み込んだ値
      */
    override def read[T](clazz:Class[T], buffer:ByteBuffer):T = {
      val longValue:Long = buffer.get() & 0xFF match {
        case b if b <= 252 => b
        case s if s == 253 => buffer.getShort & 0xFFFF
        case i if i == 254 => buffer.getInt & 0xFFFFFFFFL
        case _ => buffer.getLong
      }
      clazz.cast(LongToAny(clazz).apply(longValue))
    }
  }

  case object REAL extends DataType2(2, "real") {

    /**
      * このデータタイプでシリアライズ可能な Java/Scala 型を参照します。
      *
      * @return シリアライズ可能な型
      */
    override val compatibleTypes:Set[Class[_]] = DoubleToAny.keySet ++ LongToAny.keySet

    /**
      * 指定されたバッファに対してこのデータタイプが定義する形式で値を出力します。
      *
      * @param value 出力する値
      * @param out   出力先のバッファ
      */
    override def write(value:Any, out:DataOutputStream):Unit = {
      val doubleValue:Double = AnyToDouble.lift.apply(value).getOrElse(AnyToLong(value))
      out.writeDouble(doubleValue)
    }

    /**
      * 指定されたバッファからこのデータタイプが定義する形式で値を取得します。
      *
      * @param buffer 読み込むバッファ
      * @return 読み込んだ値
      */
    override def read[T](clazz:Class[T], buffer:ByteBuffer):T = {
      val doubleValue = buffer.getDouble
      val value = DoubleToAny.get(clazz).map(_.apply(doubleValue)).getOrElse(LongToAny(clazz).apply(doubleValue.toLong))
      clazz.cast(value)
    }
  }

  case object TEXT extends DataType2(3, "text") {

    /**
      * このデータタイプでシリアライズ可能な Java/Scala 型を参照します。
      *
      * @return シリアライズ可能な型
      */
    override val compatibleTypes:Set[Class[_]] = Set(classOf[String], classOf[java.lang.String])

    /**
      * 指定されたバッファに対してこのデータタイプが定義する形式で値を出力します。
      *
      * @param value 出力する値
      * @param out   出力先のバッファ
      */
    override def write(value:Any, out:DataOutputStream):Unit = {
      val stringValue:String = value match {
        case s:String => s
      }
      BINARY.write(stringValue.getBytes(StandardCharsets.UTF_8), out)
    }

    /**
      * 指定されたバッファからこのデータタイプが定義する形式で値を取得します。
      *
      * @param buffer 読み込むバッファ
      * @return 読み込んだ値
      */
    override def read[T](clazz:Class[T], buffer:ByteBuffer):T = {
      val stringValue = new String(BINARY.read(classOf[Array[Byte]], buffer), StandardCharsets.UTF_8)
      clazz.cast(clazz match {
        case c if c == classOf[String] => stringValue
        case c if c == classOf[java.lang.String] => java.lang.String.valueOf(stringValue)
      })
    }
  }

  case object BINARY extends DataType2(4, "binary") {

    /**
      * このデータタイプでシリアライズ可能な Java/Scala 型を参照します。
      *
      * @return シリアライズ可能な型
      */
    override val compatibleTypes:Set[Class[_]] = Set(classOf[Array[Byte]])

    /**
      * 指定されたバッファに対してこのデータタイプが定義する形式で値を出力します。
      *
      * @param value 出力する値
      * @param out   出力先のバッファ
      */
    override def write(value:Any, out:DataOutputStream):Unit = value match {
      case binary:Array[Byte] =>
        INTEGER.write(binary.length, out)
        out.write(binary)
    }

    /**
      * 指定されたバッファからこのデータタイプが定義する形式で値を取得します。
      *
      * @param buffer 読み込むバッファ
      * @return 読み込んだ値
      */
    override def read[T](clazz:Class[T], buffer:ByteBuffer):T = {
      val size = INTEGER.read(classOf[Int], buffer)
      val binary = new Array[Byte](size)
      buffer.get(binary)
      clazz.cast(binary)
    }
  }

  private[DataType2] val AnyToLong:PartialFunction[Any, Long] = {
    case null => 0L
    case b:Byte => b & 0xFF
    case c:Char => c
    case s:Short => s & 0xFFFF
    case i:Int => i & 0xFFFFFFFFL
    case l:Long => l
    case b:Boolean => if(b) 1 else 0
    case b:java.lang.Byte => b & 0xFF
    case c:Character => c.toInt
    case s:java.lang.Short => s & 0xFFFF
    case i:Integer => i & 0xFFFFFFL
    case l:java.lang.Long => l
    case b:java.lang.Boolean => if(b) 1 else 0
    case s:String => s.toLong
  }

  private[DataType2] val LongToAny:Map[Class[_], (Long) => Any] = Map(
    classOf[Boolean] -> { l:Long => l != 0 },
    classOf[Byte] -> { l:Long => l.toByte },
    classOf[Char] -> { l:Long => l.toChar },
    classOf[Short] -> { l:Long => l.toShort },
    classOf[Int] -> { l:Long => l.toInt },
    classOf[Long] -> identity,
    classOf[java.lang.Boolean] -> { l:Long => l != 0 },
    classOf[java.lang.Byte] -> { l:Long => java.lang.Byte.valueOf(l.toByte) },
    classOf[Character] -> { l:Long => Character.valueOf(l.toChar) },
    classOf[java.lang.Short] -> { l:Long => java.lang.Short.valueOf(l.toShort) },
    classOf[Integer] -> { l:Long => Integer.valueOf(l.toInt) },
    classOf[java.lang.Long] -> { l:Long => java.lang.Long.valueOf(l) },
    classOf[String] -> { l:Long => l.toString },
    classOf[java.lang.String] -> { l:Long => java.lang.String.valueOf(l.toString) }
  )

  private[DataType2] val AnyToDouble:PartialFunction[Any, Double] = {
    case null => Double.NaN
    case f:Float => f
    case d:Double => d
    case f:java.lang.Float => f.toDouble
    case d:java.lang.Double => d
    case s:String => s.toDouble
  }

  private[DataType2] val DoubleToAny:Map[Class[_], (Double) => Any] = Map(
    classOf[Float] -> { l:Double => l != l.toFloat },
    classOf[Double] -> identity,
    classOf[java.lang.Float] -> { l:Double => java.lang.Float.valueOf(l.toFloat) },
    classOf[java.lang.Double] -> { l:Double => java.lang.Double.valueOf(l) },
    classOf[String] -> { l:Double => l.toString },
    classOf[java.lang.String] -> { l:Double => java.lang.String.valueOf(l.toString) }
  )

}
