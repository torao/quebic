# Quebic - Simple Journaling Queue Library

**Quebic** is a simple and small journaling queue library based on local filesystem for Scala 2.12. It is aimed for
using this in pipeline data processing especially machine learning or ETL. Quebic supports 1:1 queue operation for
multi-thread or multi-process environment.

Once a data is pushed into queue, it will be able to read at the next time even if the process goes down. In addition,
the last pushed data remains in queue and it can refer anytime even if the queue is empty. This allows your
application's process to determine where it should resume from. You can also use this queue files as intermediate data
store for the next data processing step.

This library is implemented with a double-stacking queue using only two files. Therefore, there may be little severe
impact of file lock as far as you use this as 1:1.

## Features

* Persistent queue that can stop and restart processing.
* Process large amounts of data that exceed JavaVM heap.
* Remember the latest push data to determine restart position.
* Compression option for efficient storing large JSON or vector.
* Data type verification with simple schema.
* Simple two-files data store.
* 1:1 lock-based parallel push and pop.

## How to Use

Currently Quebic is designed for Scala 2.12. You may setup it to your project from the
[Maven Central Repository](https://search.maven.org/#search%7Cga%7C1%7Ca%3A%22quebic_2.12%22) by `sbt`, `mvn`,
`gradle` and so on.

```build.sbt
libraryDependencies += "at.hazm" % "quebic_2.12" % "1.0.+"
```

When you wish to use Quebic in your project, first you should determine the data type (specified by `T`) that you want
to push to queue.

```scala
case class MyItem(id:Int, text:String)
```

Second, you should define schema as `Schema` according to your data type and implement converter between type `MyItem`
and `Struct`. The `DataType` sequence in schema must be same as `Struct` values.

```scala
import at.hazm.quebic.{DataType, Schema, Struct}

case object MyItem2Struct extends Queue.Value2Struct[MyItem] {
  def schema:Schema = Schema(DataType.INTEGER, DataType.TEXT)

  def from(data:MyItem):Struct = Struct(Struct.INTEGER(data.id), Struct.TEXT(data.text))

  def to(struct:Struct):MyItem = struct match {
    case Struct(Struct.INTEGER(id), Struct.TEXT(text)) => MyItem(id.toInt, text)
  }
}
```

`DataType` supports simple `INTEGER`, `REAL`, `TEXT`, `BINARY` and `TENSOR` five types.

Note that the typical container type, such as `list` or `map` are not supported. If you wish to use such complex type,
you may use JSON or XML string as `TEXT`, or object serialization as `BINARY` instead of default types.

And final, you will create queue and publisher or subscriber.

```scala
import java.io.File
import java.util.Timer
import at.hazm.quebic.{Codec, Queue}

val timer = new Timer("ScheduledMigrationThread")   // scheduled migration thread
val file = new File("my-queue.qbc")
val capacity = 64 * 1024
val queue = new Queue(file, capacity, MyItem2Struct, timer)

val pub = new queue.Publisher(Codec.PLAIN)
pub.push(MyItem(0, "hello, world"))

val sub = new queue.Subscriber()
val myItem = sub.pop()

// When your program going to finish.
queue.close()
timer.cancel()

// If you wish to delete all queue-related files.
// queue.dispose()
```

### Reference Score of Performance

Followings are the score of push and pop operation on DELL OptiPlex 7050 with Windows 10, Core i7-7700 3.6GHz CPU, 16GB
memory, NTFS on HDD (not SSD).

In single thread, push - migration - pop sequential operation for 1kB binary item:

| Operation | Call  | Time     | Time/Call |
|:----------|------:|---------:|----------:|
| PUSH      | 2,781 | 10,002ms | 3.597ms   |
| POP       | 2,781 | 6,114ms  | 2.198ms   |
| MIGRATION | 2,782 | 52ms     | 0.019ms   |

## Internal Implementation

### double-stack queue

Quebic is based on double-stack queue.

![double-stack queue](doc/double-stack_queue.png)

When one client pushes an element to the queue, it will be pushed into stack-1 that works as journal. After that, if
stack-2 is empty when the other client pops from the queue, the queue first pops all elements from stack-1 and pushes
them to stack-2. So the queue behaves as FIFO.

### reliability

The basic behaviour of Quebic is the same as the primitive transaction. It always rewrites the link pointer to the data
entity after writing it. Therefore, if the system goes down while writing data entity, useless space will remain in the
file but existing data is not damaged. However, unfortunately the system goes down while updating several bytes in the 8
bytes link pointer, the sequence of enqueued data in the file may be destroyed. It is able to detect such "broken link
pointer" situation because the data header linked by pointer begins with fixed magic number.

This is very rarely situation (in fact I have never encountered) but you should not store the persistent data entities
but intermediate one that can be rebuilt by rerun in this queue.
