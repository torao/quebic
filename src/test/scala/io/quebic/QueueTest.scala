package io.quebic

import java.io.File
import java.util.Timer

import io.quebic.Queue.Value2Struct
import org.specs2.Specification
import org.specs2.execute.Result
import org.specs2.specification.core.SpecStructure

import scala.util.Random

class QueueTest extends Specification{
  override def is:SpecStructure = s2"""
initialized status: $initialState
push and pop in single thread: $normalPushAndPopInSingleThread
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
    fileIsEmpty and initialQueueSizeIsZero and fileCanDelete
  }

  def normalPushAndPopInSingleThread:Result = {
    val file = File.createTempFile("test-", ".qbc", new File("."))
    file.deleteOnExit()
    val capacity = 500
    val queue = new Queue[String](file, capacity, String2Struct, timer)
    val expected = (0 until capacity).map{ i => randomString(i, i) }
    val publish = new queue.Publisher()
    val subscribe = new queue.Subscriber()
    expected.foreach{ s => publish.push(s) }
    System.out.println(f"${expected.length} pushed queue size: ${queue.diskSpace}%,dB")
    val pushedSizeEqualsItemCount = queue.size === expected.length
    val actual = expected.indices.map{ _ => subscribe.pop() }
    System.out.println(f"${actual.length} popped queue size: ${queue.diskSpace}%,dB")
    val elementAllPoped = queue.size === 0
    val poppedElementsAreAllRetrieved = actual.length === expected.length
    val poppedElementsAreAllEquals = actual.zip(expected).map(x => x._1 === x._2).reduceLeft(_ and _)
    queue.close()
    val fileCanDelete = file.delete() must beTrue

    pushedSizeEqualsItemCount and elementAllPoped and poppedElementsAreAllRetrieved and poppedElementsAreAllEquals and fileCanDelete
  }

  object String2Struct extends Value2Struct[String] {
    override def schema:Schema = Schema(DataType.TEXT)

    override def from(value:String):Struct = Struct(Struct.TEXT(value))

    override def to(value:Struct):String = value.values.head.asInstanceOf[Struct.TEXT].value
  }

  val timer = new Timer("QueueTest", true)

  def randomString(seed:Long, length:Int):String = new Random(seed).nextString(length)

}
