package at.hazm.quebic

import java.io.{InputStream, OutputStream}

import org.slf4j.LoggerFactory

import scala.annotation.tailrec

object ScriptRunner {
  private[ScriptRunner] val logger = LoggerFactory.getLogger(getClass.getName.dropRight(1))

  def execJar(cmd:String, args:String*):Process = {
    val c = (Seq("java", "-jar", cmd) ++ args).toArray
    logger.info(c.mkString(" "))
    new ProcessBuilder()
      .command(c:_*)
      .inheritIO()
      .start()
  }

  def copy(in:InputStream, out:OutputStream):Long = {
    @tailrec
    def _copy(buffer:Array[Byte], length:Long = 0):Long = {
      val len = in.read(buffer)
      if(len < 0) length else {
        out.write(buffer, 0, len)
        out.flush()
        _copy(buffer, length + len)
      }
    }

    _copy(new Array[Byte](1024))
  }

}
