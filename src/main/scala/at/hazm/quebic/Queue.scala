package at.hazm.quebic

import java.io.{File, IOException}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.{Timer, TimerTask}

import at.hazm.quebic.Queue.{Value2Struct, logger, using}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec

/**
  * スレッドやプロセス間で共有できるローカルファイルを使用したキューです。JavaVM ヒープを使用する Java/Scala 標準のコレクション API
  * より多数で大きなサイズのデータを扱うことができます。
  *
  * キューはスキーマ (タプル形式のデータ型) を定義できます。このスキーマを使用してキューの投入側と取得側で安全にデータを変換することが
  * できます。任意のユーザ定義データを透過的に扱うために構築時に `Struct` へのコンバーター実装である `Value2Struct` を指定します。
  *
  * @param file                  キューに使用するファイル
  * @param capacity              キューの容量
  * @param conv                  任意のデータを [[Struct]] 型に変換するコンバーター
  * @param timer                 ジャーナルからキューへデータの移行処理を行うタイマー
  * @param autoMigrationInterval 自動マイグレーションの起動間隔 (ミリ秒)
  * @tparam T このキューに投入または取り出す型
  */
class Queue[T](val file:File, val capacity:Long, conv:Value2Struct[T], timer:Timer, autoMigrationInterval:Long = 1000L) extends AutoCloseable {
  if(capacity <= 0) {
    throw new IllegalArgumentException(f"queue capacity $capacity%,d must be larger than zero")
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

  timer.scheduleAtFixedRate(migrateTask, autoMigrationInterval, autoMigrationInterval)

  private[this] val closed = new AtomicBoolean(false)

  /**
    * ジャーナルとキューの両方に対して処理を実行します。IMPORTANT: デッドロック回避のためマイグレーション時のロック順は journal → queue
    * にすること。
    *
    * @param f ジャーナルとキューの両方に対して行う処理
    * @tparam R 処理結果の型
    * @return 処理結果
    */
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
    // IMPORTANT: デッドロック回避のためマイグレーション時のロック順は journal → queue にすること。
    if(journalFile.length() > 0) {
      on(journalFile, journalFileLock) { in =>
        on(file, queueFileLock) { out =>
          in.migrateTo(out)
        }
      }
    }
  }

  /**
    * このキューに保存されているデータ数を参照します。
    *
    * @return キューのデータ数
    */
  def size:Long = if(journalFile.length() < 0) {
    queue(_.size)
  } else both { (a, b) => a.size + b.size }

  /**
    * このキューが空であるかを判定します。
    *
    * @return キューが空の場合 true
    */
  def isEmpty:Boolean = size == 0

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

  /**
    * このキューをストレージから削除します。キューがまだクローズしていない場合は暗黙的にクローズします。
    * 他のスレッドまたはプロセスがキューを使用している場合は削除に失敗したり、またはデータ欠損が発生する可能性があります。
    */
  def dispose():Unit = {
    close()
    file.delete()
    journalFile.delete()
  }

  /**
    * このキューにデータを追加するためのインターフェースです。
    *
    * @param compression データ追加時の圧縮形式 (デフォルトは無圧縮)
    */
  class Publisher(compression:Codec = Codec.PLAIN) {

    /**
      * このキューへのデータ追加を試行します。キューの保持しているデータ数が `capacity` に達している場合、メソッドは直ちに false を
      * 返します。
      *
      * @param value    追加するデータ
      * @param lifetime データの有効期限 (現在時刻からのミリ秒)、無期限の場合は負の値
      * @return データを追加できた場合 true
      */
    def tryPush(value:T, lifetime:Long = -1):Boolean = journal { j =>
      // IMPORTANT: デッドロック回避のためマイグレーション時のロック順は journal → queue にすること。
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
        // IMPORTANT: デッドロック回避のためマイグレーション時のロック順は journal → queue にすること。
        val remaining = journal { j =>
          val permitCount = math.min(capacity - (queue(_.size) + j.size), values.length).toInt
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

    /**
      * このキューに追加された最も新しいデータを参照します。予期せぬ状況で終了した処理の再開時にキューへの投入がどこまで進んでいたかを
      * 調べる目的で、キューが空となった場合でも直近の追加データはキュー内に保持されています。
      *
      * @return このキューに追加された最も新しいデータ
      */
    def latest:Option[T] = both { case (j, q) =>
      (if(j.size > 0) j.peek else q.peekDeepest).map(conv.to)
    }
  }

  class Subscriber() {

    /**
      * このキューからデータの取り出しを試行します。キュー内に有効なデータが存在した場合 `Some(data)` を返します。キューが空の場合は
      * `None` を返します。このメソッドは呼び出し後直ちに終了します。
      *
      * @return キューから取り出したデータ、または `None`
      */
    def tryPop():Option[T] = queue { q =>
      if(q.size > 0) {
        q.pop(0).map(conv.to)
      } else None
    }.orElse {
      // ロック順を調整するためにいったんロックを外してマイグレーションを実行
      if(journalFile.length() <= 0) {
        None
      } else both { case (j, q) =>
        j.migrateTo(q)
        if(q.size > 0) q.pop(0).map(conv.to) else None
      }
    }

    /**
      * このキューからデータを取り出します。このメソッドは `pop(-1).get` と等価です。
      *
      * @return キューから取り出したデータ
      */
    def pop():T = pop(-1).get

    /**
      * このキューからデータを取り出します。キューに有効なデータが存在しない場合 `limit` (ミリ秒) が経過するまでデータの到着を待機します。
      * `limit` までにデータが到着した場合 `Some(result)` を返します。`limit` を超えてもデータが到着しなかった場合は `None` を
      * 返します。`limit` に負の値を指定した場合は無制限となります。
      *
      * @param limit データが存在しなかった場合にデータ到着まで待機する時間 (ミリ秒)
      * @return キューから取り出したデータ、または `None`
      */
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