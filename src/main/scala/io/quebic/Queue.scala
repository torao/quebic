package io.quebic

import java.io.{File, IOException}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.{Timer, TimerTask}

import io.quebic.Queue.{Value2Struct, logger, using}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec

class Queue[T](val file:File, val capacity:Long, conv:Value2Struct[T], timer:Timer) extends AutoCloseable {
  if(capacity <= 0) {
    throw new IllegalArgumentException(f"queue capacity must be larger than zero: $capacity%,d")
  }

  /** ジャーナルファイル */
  private[this] val journalFile = {
    val name = file.getName
    val ext = name.lastIndexOf('.')
    val base = if(ext >= 0) name.substring(0, ext) else name
    new File(file.getAbsoluteFile.getParent, s"$base.qbj")
  }

  /**
    * 同一 JVM 内で同じジャーナルファイルにアクセスしないための synchronized 用オブジェクト。`FileChannel.lock()` が同一
    * JVM 上でのロックを OverlappingFileLockException とするため。
    */
  private[this] val journalFileLock = journalFile.getCanonicalPath.intern()

  /**
    * 同一 JVM 内で同じキューファイルにアクセスしないための synchronized 用オブジェクト。
    */
  private[this] val queueFileLock = file.getCanonicalPath.intern()

  /**
    * ジャーナルからキューへ定期マイグレーションを行うためのタスク。
    */
  private[this] val migrateTask = new TimerTask {
    override def run():Unit = try {
      migrate()
    } catch {
      case ex:Throwable =>
        logger.error(s"migration failed: $journalFile -> $file", ex)
    }
  }

  timer.scheduleAtFixedRate(migrateTask, 1000, 1000)

  private[this] val closed = new AtomicBoolean(false)

  private[this] def both[R](f:(JournaledFile, JournaledFile) => R):R = journal { in =>
    queue { out =>
      f(in, out)
    }
  }

  private[this] def on[R](file:File, lock:Object)(f:(JournaledFile) => R):R = lock.synchronized {
    using(new JournaledFile(file, conv.schema)) { journal =>
      f(journal)
    }
  }

  private[this] def journal[R](f:(JournaledFile) => R):R = if(closed.get) {
    throw new IOException(s"queue closed: $file")
  } else on(journalFile, journalFileLock) { file =>
    f(file)
  }

  private[this] def queue[R](f:(JournaledFile) => R):R = if(closed.get) {
    throw new IOException(s"queue closed: $file")
  } else on(file, queueFileLock) { file =>
    f(file)
  }

  /**
    * バッチマイグレーションの実行。このメソッドはキューがクローズされた後にマイグレーションを行うため `closed` フラグを
    * 無視する。
    */
  private[this] def migrate():Unit = {
    // 重要: デッドロック回避のためマイグレーション時のロック順は queue → journal にすること。
    if(journalFile.length() > 0) {
      on(file, queueFileLock){ out =>
        on(journalFile, journalFileLock) { in =>
          in.migrateTo(out)
        }
      }
    }
    if(closed.get() && journalFile.exists()) {
      journalFile.delete()
    }
  }

  /**
    * このキューに保存されているデータ数を参照します。
    *
    * @return キューのデータ数
    */
  def size:Long = if(journalFile.length() < 0){
    queue(_.size)
  } else both { (a, b) => a.size + b.size }

  /**
    * このキューが使用しているディスクスペースを参照します。
    *
    * @return ディスクスペース (バイト)
    */
  def diskSpace:Long = math.max(0, file.length()) + math.max(0, journalFile.length())

  /**
    * このキューが使用しているリソースを解放します。キューから取り出されていないデータはファイルに永続化されています。
    */
  def close():Unit = if(closed.compareAndSet(false, true)) {
    migrateTask.cancel()
    migrateTask.run()
  }

  class Publisher(compression:CompressionType = CompressionType.NONE) {

    /**
      * このキューへのデータ追加を試行します。キューの保持しているデータ数が `capacity` に達している場合、メソッドは直ちに false を
      * 返します。
      *
      * @param value    追加するデータ
      * @param lifetime データの有効期限 (現在時刻からのミリ秒)、無期限の場合は負の値
      * @return データを追加できた場合 true
      */
    def tryPush(value:T, lifetime:Long = -1):Boolean = journal { j =>
      if(queue(_.size) + j.size >= capacity) false else {
        j.push(conv.from(value), lifetime, compression)
        true
      }
    }

    /**
      * このキューへデータを追加します。キューの保持しているデータ数が `capacity` に達している場合、メソッドはデータが投入できるまで
      * 指定時間まで待機します。指定時間が経過してもデータが追加できなかった場合 false が返されます。
      *
      * @param value    キューに追加するデータ
      * @param limit    データ追加の待機限界時間 (ミリ秒)、無制限の場合は負の値
      * @param lifetime データの有効期限 (現在時刻からのミリ秒)、無期限の場合は負の値
      * @return データを追加できた場合 true
      */
    def push(value:T, limit:Long = -1, lifetime:Long = -1):Boolean = {
      @tailrec
      def _push(t0:Long):Boolean = {
        if(tryPush(value, lifetime)) true else {
          Thread.sleep(200)
          if(limit >= 0 && (System.nanoTime() - t0) / 1000 / 1000 > limit) false else _push(t0)
        }
      }

      _push(System.nanoTime())
    }

    /**
      * 指定されたすべてのデータを先頭からこのキューへ追加します。キューの保持しているデータ数が `capacity` に達した場合、メソッドは
      * データが投入できるまで指定時間まで待機します。指定時間が経過してもデータが追加できなかった場合、追加できなかったデータが返されます。
      *
      * @param values   キューに追加するデータ
      * @param limit    データ追加の待機限界時間 (ミリ秒)、無制限の場合は負の値
      * @param lifetime データの有効期限 (現在時刻からのミリ秒)、無期限の場合は負の値
      * @return 待機限界時間が過ぎてもキューに追加できなかったデータ
      */
    def pushAll(values:Seq[T], limit:Long = -1, lifetime:Long = -1):Seq[T] = {
      @tailrec
      def _pushAll(t0:Long, values:Seq[T]):Seq[T] = {
        val remaining = journal { j =>
          val permitCount = math.min(queue(_.size) + j.size, values.length).toInt
          if(permitCount > 0) {
            val (permitted, remaining) = values.splitAt(permitCount)
            permitted.foreach(value => j.push(conv.from(value), lifetime, compression))
            remaining
          } else values
        }
        if(remaining.isEmpty) Seq.empty else {
          Thread.sleep(200)
          if(limit >= 0 && (System.nanoTime() - t0) / 1000 / 1000 > limit) remaining else _pushAll(t0, remaining)
        }
      }

      _pushAll(System.nanoTime(), values)
    }
  }

  class Subscriber() {

    def tryPop():Option[T] = queue { q =>
      if(q.size > 0) {
        q.pop(0).map(conv.to)
      } else if(journalFile.length() <= 0) {
        None
      } else {
        // 重要: デッドロック回避のためマイグレーション時のロック順は queue → journal にすること。
        journal(_.migrateTo(q))
        if(q.size > 0) q.pop(0).map(conv.to) else None
      }
    }

    def pop():T = pop(-1).get

    def pop(limit:Long):Option[T] = {
      @tailrec
      def _pop(t0:Long):Option[T] = {
        val result = tryPop()
        if(result.isDefined) result else {
          Thread.sleep(200)
          if(limit >= 0 && (System.nanoTime() - t0) / 1000 / 1000 > limit) None else _pop(t0)
        }
      }

      _pop(System.nanoTime())
    }
  }

}

object Queue {
  private[Queue] val logger = LoggerFactory.getLogger(classOf[Queue[_]])

  private[Queue] def using[R <: AutoCloseable, U](r:R)(f:(R) => U):U = try {
    f(r)
  } finally {
    r.close()
  }

  trait Value2Struct[T] {
    def schema:Schema

    def from(value:T):Struct

    def to(value:Struct):T
  }

}