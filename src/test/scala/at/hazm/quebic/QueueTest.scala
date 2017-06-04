package at.hazm.quebic

import java.io.File
import java.util.Timer

import Queue.Value2Struct
import org.specs2.Specification
import org.specs2.execute.Result
import org.specs2.specification.core.SpecStructure

import scala.collection.mutable
import scala.util.Random

class QueueTest extends Specification {
  override def is:SpecStructure =
    s2"""
initialized status: $initialState
push and pop in single thread: $normalPushAndPopInSingleThread
push and pop in multi threads: $multiThreadingPushAndPop
GZIP compressed push and pop:  ${compression(Codec.GZIP)}
"""

  def initialState:Result = {
    val file = File.createTempFile("test-", ".qbc", new File("."))
    file.deleteOnExit()
    val capacity = 100
    val queue = new Queue[String](file, capacity, String2Struct, timer)
    val fileIsEmpty = file.length() == 0
    val initialQueueSizeIsZero = queue.size === 0
    queue.close()
    val fileCanDelete = file.delete() must beTrue
    queue.dispose()
    fileIsEmpty and initialQueueSizeIsZero and fileCanDelete
  }

  def normalPushAndPopInSingleThread:Result = {
    val file = File.createTempFile("test-", ".qbc", new File("."))
    file.deleteOnExit()
    val capacity = 500
    val queue = new Queue[String](file, capacity, String2Struct, timer)
    val expected = (0 until capacity).map { i => randomString(i, i) }
    val publish = new queue.Publisher()
    val subscribe = new queue.Subscriber()
    val t0 = System.currentTimeMillis()
    expected.foreach { s => publish.push(s) }
    val t1 = System.currentTimeMillis()
    System.out.println(f"${expected.length} pushed queue size: ${queue.diskSpace}%,dB [${t1 - t0}%,dms]")
    val pushedSizeEqualsItemCount = queue.size === expected.length
    val actual = expected.indices.map { _ => subscribe.pop() }
    val t2 = System.currentTimeMillis()
    System.out.println(f"${actual.length} popped queue size: ${queue.diskSpace}%,dB [${t2 - t1}%,dms]")
    val elementAllPoped = queue.size === 0
    val poppedElementsAreAllRetrieved = actual.length === expected.length
    val poppedElementsAreAllEquals = actual.zip(expected).map(x => x._1 === x._2).reduceLeft(_ and _)
    queue.dispose()

    pushedSizeEqualsItemCount and elementAllPoped and poppedElementsAreAllRetrieved and poppedElementsAreAllEquals
  }

  def multiThreadingPushAndPop:Result = {
    val file = File.createTempFile("test-", ".qbc", new File("."))
    file.deleteOnExit()

    val capacity = 500
    val expected = capacity.range.map { i => randomString(i, i) }.toList
    val actual = mutable.Buffer[String]()

    val ready = new Object()
    val signal = new Object()

    val pushThreadSize = 5
    val popThreadSize = 5
    assert((expected.length * pushThreadSize) % popThreadSize == 0)

    val popper:Runnable = () => {
      val queue = new Queue[String](file, capacity, String2Struct, timer)
      signal.synchronized {
        ready.synchronized(ready.notify())
        signal.wait()
      }
      val subscribe = new queue.Subscriber()
      val t0 = System.currentTimeMillis()
      val data = (expected.length * pushThreadSize / popThreadSize).fill(subscribe.pop(5 * 1000L)).flatten.toList
      System.out.println(f"  POP: ${expected.length}%,d, ${System.currentTimeMillis() - t0}%,d[ms]")
      actual.synchronized(actual.appendAll(data))
      queue.close()
    }

    val pusher:Runnable = () => {
      val queue = new Queue[String](file, capacity, String2Struct, timer)
      signal.synchronized {
        ready.synchronized(ready.notify())
        signal.wait()
      }
      val publish = new queue.Publisher()
      val t0 = System.currentTimeMillis()
      expected.foreach(s => publish.push(s))
      System.out.println(f"  PUSH: ${expected.length}%,d, ${System.currentTimeMillis() - t0}%,d[ms]")
      queue.close()
    }

    val t0 = System.currentTimeMillis()
    val threads = (popThreadSize.range.map { i => (s"popper-$i", popper) } ++ pushThreadSize.range.map { i => (s"pusher-$i", pusher) }).map { case (name, r) =>
      val thread = new Thread(r, name)
      thread.setDaemon(true)
      ready.synchronized {
        thread.start()
        ready.wait()
      }
      thread
    }
    signal.synchronized(signal.notifyAll())
    threads.foreach(_.join(60 * 1000))
    val t = System.currentTimeMillis() - t0
    System.out.println(f"push ${pushThreadSize * expected.length}%,d, pop ${actual.length}%,d [$t%,dms]")

    actual.groupBy(identity).mapValues(_.length).filter(_._2 != pushThreadSize).foreach { case (s, i) =>
      System.out.println(f"$i%,d = $s%s")
    }
    val queue = new Queue[String](file, capacity, String2Struct, timer)
    val queueIsEmpty = queue.size === 0
    queue.close()
    queue.dispose()
    val popSameCountToPush = actual.length === expected.length * pushThreadSize
    val popSameDataToPush = actual.distinct.length == expected.length
    val allPopDataIsPushCount = actual.groupBy(identity).mapValues(_.length).values.map(_ === pushThreadSize).reduceLeft(_ and _)
    queueIsEmpty and popSameCountToPush and popSameDataToPush and allPopDataIsPushCount
  }

  def compression(compress:Codec):Result = {
    val file = File.createTempFile("test-", ".qbc", new File("."))
    file.deleteOnExit()
    val capacity = 10
    val queue = new Queue[String](file, capacity, String2Struct, timer)
    val expected = (0 until capacity).map { i => randomString(i, i * 1024 * 1024) }

    val publish = new queue.Publisher(compress)
    val subscribe = new queue.Subscriber()

    val t0 = System.currentTimeMillis()
    expected.foreach { s => publish.push(s) }
    val t1 = System.currentTimeMillis()
    System.out.println(f"${compress.name}: ${expected.length} pushed queue size: ${queue.diskSpace}%,dB [${t1 - t0}%,dms]")

    val actual = expected.indices.map { _ => subscribe.pop() }
    val t2 = System.currentTimeMillis()
    System.out.println(f"${compress.name}: ${actual.length} popped queue size: ${queue.diskSpace}%,dB [${t2 - t1}%,dms]")

    val elementAllPoped = queue.size === 0
    val poppedElementsAreAllRetrieved = actual.length === expected.length
    val poppedElementsAreAllEquals = actual.zip(expected).map(x => x._1 === x._2).reduceLeft(_ and _)
    queue.dispose()

    elementAllPoped and poppedElementsAreAllRetrieved and poppedElementsAreAllEquals
  }

  object String2Struct extends Value2Struct[String] {
    override def schema:Schema = Schema(DataType.TEXT)

    override def from(value:String):Struct = Struct(Struct.TEXT(value))

    override def to(value:Struct):String = value.values.head.asInstanceOf[Struct.TEXT].value
  }

  val timer = new Timer("QueueTest", true)

  def randomString(seed:Long, length:Int):String = {
    val sample = (('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9') ++ ('ぁ' to 'ゖ')).mkString
    val rand = new Random(seed)
    length.fill(sample(rand.nextInt(sample.length))).mkString
  }

  implicit class _Number(num:Int) {
    def times(f: => Unit):Unit = range.foreach(_ => f)

    def fill[T](f: => T):Iterable[T] = range.map(_ => f)
    def range:Iterable[Int] = 0 until num
  }

}
