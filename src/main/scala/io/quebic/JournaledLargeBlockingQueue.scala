package io.quebic

import java.io.File
import java.util
import java.util.concurrent.{BlockingQueue, TimeUnit}

import JournaledBlockingQueue._
import io.quebic.JournaledBlockingQueue.Value2Struct

class JournaledLargeBlockingQueue[T](file:File, capacity:Long, conv:Value2Struct[T]) extends BlockingQueue[T] {
  private[this] val journal = new File(file.getAbsoluteFile.getParent, file.getName + ".journal")
  private[this] val schema = conv.schema

  private[this] def both[R](f:(JournaledFile,JournaledFile)=>R):R = using(new JournaledFile(journal, schema, false, capacity)){ in =>
    using(new JournaledFile(file, schema, true, capacity)) { out =>
      f(in, out)
    }
  }

  private[this] def migrate():Unit = both(_ migrateTo _)

  override def poll(timeout:Long, unit:TimeUnit):Struct = ???

  override def remove(o:scala.Any):Boolean = ???

  override def put(e:T):Unit = ???

  override def offer(e:T):Boolean = ???

  override def offer(e:T, timeout:Long, unit:TimeUnit):Boolean = ???

  override def add(e:T):Boolean = ???

  override def drainTo(c:util.Collection[_ >: T]):Int = ???

  override def drainTo(c:util.Collection[_ >: T], maxElements:Int):Int = ???

  override def take():T = ???

  override def contains(o:scala.Any):Boolean = ???

  override def remainingCapacity():Int = ???

  override def poll():T = ???

  override def remove():T = ???

  override def element():T = ???

  override def peek():T = ???

  override def iterator():util.Iterator[T] = ???

  override def removeAll(c:util.Collection[_]):Boolean = ???

  override def toArray:Array[AnyRef] = ???

  override def toArray[T](a:Array[T]):Array[T] = ???

  override def containsAll(c:util.Collection[_]):Boolean = ???

  override def clear():Unit = ???

  override def isEmpty:Boolean = ???

  override def size():Long = both{ (journal, queue) => journal.size + queue.size }

  override def addAll(c:util.Collection[_ <: T]):Boolean = ???

  override def retainAll(c:util.Collection[_]):Boolean = ???
}

object JournaledLargeBlockingQueue {

  private[quebic] def using[R <: AutoCloseable, T](r:R)(f:(R) => T):T = try {
    f(r)
  } finally {
    r.close()
  }

  trait Value2Struct[T] {
    def schema:Schema

    def from(value:T):Struct

    def to(data:Struct):T
  }

}