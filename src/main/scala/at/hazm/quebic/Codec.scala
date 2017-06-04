package at.hazm.quebic

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import scala.annotation.tailrec

sealed abstract class Codec(val id:Byte, val name:String) extends Type {
  def encode(buffer:Array[Byte]):Array[Byte]

  def decode(buffer:Array[Byte]):Array[Byte]
}

object Codec {
  val values:Seq[Codec] = Seq(PLAIN, GZIP)
  private[this] val valuesMap = values.groupBy(_.id).mapValues(_.head)

  def valueOf(id:Byte):Codec = valuesMap(id)

  case object PLAIN extends Codec(0, "plain") {
    def encode(buffer:Array[Byte]):Array[Byte] = buffer

    def decode(buffer:Array[Byte]):Array[Byte] = buffer
  }

  case object GZIP extends Codec(1, "gzip") {
    def encode(buffer:Array[Byte]):Array[Byte] = {
      val baos = new ByteArrayOutputStream()
      val out = new GZIPOutputStream(baos)
      out.write(buffer)
      out.finish()
      out.finish()
      baos.toByteArray
    }

    def decode(buffer:Array[Byte]):Array[Byte] = {
      val in = new GZIPInputStream(new ByteArrayInputStream(buffer))
      val out = new ByteArrayOutputStream()
      _copy(in, out, new Array[Byte](2014))
      out.close()
      out.toByteArray
    }
  }

  @tailrec
  private[Codec] def _copy(in:InputStream, out:OutputStream, buffer:Array[Byte]):Unit = {
    val len = in.read(buffer)
    if(len > 0) {
      out.write(buffer, 0, len)
      _copy(in, out, buffer)
    }
  }

}
