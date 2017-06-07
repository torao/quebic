package at.hazm.quebic

import java.io.File
import java.util.Timer

import at.hazm.quebic.Queue.Value2Struct

object Performance {

  def main(args:Array[String]):Unit = {
    val file = File.createTempFile("test-", ".qbc", new File("."))
    file.deleteOnExit()
    val timer = new Timer("")
    val capacity = Long.MaxValue
    val queue = new Queue[Array[Byte]](file, capacity, Bin2Struct, timer, 60 * 60 * 1000L)

    val sample = new Array[Byte](1024)

    locally {
      val pub = new queue.Publisher(Codec.PLAIN)
      val t0 = System.currentTimeMillis()
      var count = 0L
      var t = 0L
      while(System.currentTimeMillis() <= t0 + 10 * 1000L){
        val t1 = System.currentTimeMillis()
        pub.push(sample)
        t += System.currentTimeMillis() - t1
        count += 1
      }
      System.out.println(f"PUSH: $t%,d[msec] / $count%,d[op] = ${t / count.toDouble}%,.3f[msec/op]")
      pub.push(sample)    // pop 計測前に強制的に migration を起動させるためのゴミデータ
    }

    locally {
      val sub = new queue.Subscriber()
      sub.pop()     // 強制的に migration を起動
      var count = 0L
      var t = 0L
      while(! queue.isEmpty){
        val t1 = System.currentTimeMillis()
        sub.pop()
        t += System.currentTimeMillis() - t1
        count += 1
      }
      System.out.println(f"POP: $t%,d[msec] / $count%,d[op] = ${t / count.toDouble}%,.3f[msec/op]")
    }

    queue.close()
    timer.cancel()
    queue.dispose()
  }

  object Bin2Struct extends Value2Struct[Array[Byte]] {
    override def schema:Schema = Schema(DataType.BINARY)

    override def from(value:Array[Byte]):Struct = Struct(Struct.BINARY(value))

    override def to(value:Struct):Array[Byte] = value match {
      case Struct(Struct.BINARY(bin)) => bin
      case _ => throw new IllegalStateException("unexpected data detected")
    }
  }
}
