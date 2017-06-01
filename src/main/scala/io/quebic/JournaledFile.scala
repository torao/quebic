package io.quebic

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption

import io.quebic.JournaledFile.offset

import scala.annotation.tailrec

private[quebic] class JournaledFile(file:File, schema:Schema, fifo:Boolean, capacity:Long) extends AutoCloseable {
  private[this] val path = file.toPath
  private val fc = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
  private[this] val lock = fc.lock()

  private[this] val schemaBinary = schema.toByteArray

  locally {
    if(fc.size() == 0) {
      init()
    } else {
      validate()
    }
  }

  private object raw {
    private[this] val longBuffer = ByteBuffer.allocate(java.lang.Long.BYTES)

    def readLong(offset:Long):Long = {
      longBuffer.clear()
      fc.read(longBuffer, offset)
      longBuffer.flip()
      longBuffer.getLong
    }

    def writeLong(offset:Long, value:Long):Unit = {
      longBuffer.clear()
      longBuffer.putLong(value)
      longBuffer.flip()
      fc.write(longBuffer, offset)
    }
  }

  private object header {
    val size:Int = offset.SCHEMA + schemaBinary.length

    def currentItems:Long = raw.readLong(offset.CURRENT_ITEMS)

    def currentItems_=(value:Long):Long = {
      raw.writeLong(offset.CURRENT_ITEMS, value)
      value
    }

    def lastPosition:Long = raw.readLong(offset.LAST_POSITION)

    def lastPosition_=(value:Long):Long = {
      raw.writeLong(offset.LAST_POSITION, value)
      value
    }
  }

  def size:Long = header.currentItems

  /**
    * 指定されたデータをこのジャーナルに追加します。
    *
    * データの有効期限は現在時刻からの相対時間をミリ秒で指定します。この時刻を過ぎてもキューから取り出されなかった
    * データは破棄されます。データが有効期限を持たない場合は負の値を指定します。
    *
    * @param values      ジャーナルに追加するデータ
    * @param lifetime    データの有効期限を示す現在時刻からの相対時間 (ミリ秒)。負の値を指定した場合は有効期限なし
    * @param compression データの圧縮方法
    * @return 追加を行った場合 true、このジャーナルに capacity 以上のデータが存在していて追加を行わなかった場合 false
    */
  def push(values:Struct, lifetime:Long = -1, compression:CompressionType = CompressionType.NONE):Boolean = {

    // 現在のエントリ数を取得
    val currentItems = header.currentItems
    if((currentItems < 0 && currentItems == Long.MaxValue) || currentItems >= capacity) {
      false
    } else {

      // データとエントリの書き込み
      val serializedValues = schema.serialize(values, compression)
      val previous = header.lastPosition
      val regionOffset = fc.size()
      val entryOffset = writeDataWithEntry(regionOffset, serializedValues, previous, lifetime, compression.id)

      // ヘッダの更新
      header.currentItems = currentItems + 1
      header.lastPosition = entryOffset

      fc.force(true)
      true
    }
  }

  /**
    * このジャーナルに追加された直近のデータを取得します。
    * このメソッドで取り出した時点でデータはキューから削除されます。取り出したデータに対する処理が失敗したとき、次回
    * リトライを行う目的でデータをジャーナル内に保留したい場合は [[JournaledFile.consume[T]]] を使用してください。
    *
    * @param errorPermitCount 一つのデータに許容するエラー回数。デフォルト 3
    * @return ジャーナルから取り出したデータ、または `None`
    */
  def pop(errorPermitCount:Short = 3):Option[Struct] = consume()(identity) match {
    case Left(ex) => throw ex
    case Right(result) => result
  }

  /**
    * このジャーナルに追加された直近のデータを `f` に渡します。`f` の呼び出しが正常に終了した場合にデータをジャーナル
    * から取り除き `f` の処理結果を返します。
    *
    * `f` の呼び出しで例外が発生した場合、データはジャーナルに残されたままエラーカウントのみがインクリメントされ
    * `Left[Throwable]` が返されます。エラーはパラメータで指定した `maxRetry` 回まで許容されます。つまり、エラーが発生
    * したとしても `maxRetry` に満たなければ次回の呼び出しで再度参照されます。`maxRetry` に達したデータは暗黙的に破棄
    * され次のデータが評価対象となります。
    *
    * エラーの発生は様々な要因が考えられます。リトライで解消することもありますし、
    *
    * @param errorPermitCount 一つのデータに許容するエラー回数。デフォルト 3
    * @param f                取り出したデータに対して行う処理
    * @tparam T データの処理結果
    * @return ジャーナルに有効なデータが存在し正常に処理が行われた場合 `Right(Some[T])`、ジャーナルに有効なデータが存在し
    *         ない場合 `Right(None)`、`f` の処理中に例外が発生した場合 `Left[Throwable]`
    */
  def consume[T](errorPermitCount:Short = 3)(f:(Struct) => T):Either[Throwable, Option[T]] = {
    consumeEntryWithData(errorPermitCount) { case (_, dataBuffer) =>
      f(schema.deserialize(dataBuffer))
    }
  }

  @tailrec
  private[this] def consumeEntryWithData[T](maxRetry:Short)(f:(ByteBuffer, ByteBuffer) => T):Either[Throwable, Option[T]] = {

    // 末尾のエントリ位置を読み込み
    val entryOffset = header.lastPosition
    if(entryOffset < 0) {
      Right(None)
    } else {
      assert(entryOffset > header.size)

      // 末尾のエントリとデータを読み込み
      val entryBuffer = ByteBuffer.allocate(JournaledFile.ENTRY_SIZE)
      val dataBuffer = readDataWithEntry(entryOffset, entryBuffer)

      // 処理の実行
      var retry = false
      val expiresAt = entryBuffer.getLong(offset.EXPIRES_AT)
      val result = if(expiresAt < 0 || System.currentTimeMillis() < expiresAt) {
        try {
          // デシリアライズの失敗もエラーリトライの対象とするため try-catch 内で deserialize
          Right(Some(f(entryBuffer, dataBuffer)))
        } catch {
          case ex:Throwable =>
            // エラー回数によって失敗として返すか廃棄するかを判断
            val errors = (entryBuffer.getShort(offset.ERRORS) & 0xFFFF) + 1
            entryBuffer.putShort(offset.ERRORS, errors.toShort)
            if(errors >= maxRetry) {
              retry = true
              Right(None)
            } else {
              Left(ex)
            }
        }
      } else {
        retry = true
        Right(None)
      }

      if(retry) consumeEntryWithData(maxRetry)(f) else if(result.isRight) {
        // 成功している場合はエントリを切り離しエントリ部分を切り詰め
        val previousEntryOffset = entryBuffer.getLong(offset.PREVIOUS)
        header.currentItems = if(previousEntryOffset < 0) 0 else header.currentItems = header.currentItems - 1
        header.lastPosition = previousEntryOffset
        fc.truncate(entryOffset)
        fc.force(true)
        result
      } else {
        // 失敗している場合はエントリを更新
        entryBuffer.position(0)
        assert(entryBuffer.limit() == JournaledFile.ENTRY_SIZE)
        fc.write(entryBuffer, entryOffset)
        fc.force(true)
        result
      }
    }
  }

  /**
    * このファイルを空の Quebic Journal で上書きします。
    */
  private[this] def init():Unit = {
    val buffer = ByteBuffer.allocate(header.size)
    buffer.putShort(JournaledFile.MagicNumber) // magic number
    buffer.putShort(header.size.toShort) // header size
    buffer.putLong(0L) // current items
    buffer.putLong(-1L) // last position
    buffer.put(schemaBinary) // schema
    buffer.flip()
    fc.write(buffer)
    fc.truncate(header.size)
  }

  /**
    * データとエントリを指定された位置に書き込みます。
    *
    * @param entryOffset     書き込む位置の先頭オフセット
    * @param dataBuffer      書き込むデータのバッファ
    * @param prevEntryOffset 書き込むデータの一つ手前のエントリオフセット
    * @param lifetime        データの寿命 (ミリ秒)
    * @param compression     データの圧縮形式
    * @return 書き込んだデータのエントリオフセット
    */
  private def writeDataWithEntry(entryOffset:Long, dataBuffer:ByteBuffer, prevEntryOffset:Long, lifetime:Long, compression:Byte):Long = {
    assert(dataBuffer.position() == 0)
    val now = System.currentTimeMillis()
    val dataLength = dataBuffer.limit()
    val entryBuffer = ByteBuffer.allocate(JournaledFile.ENTRY_SIZE)
    entryBuffer.putLong(prevEntryOffset) // previous entry offset from head of file
    entryBuffer.putLong(now) // created at as milli sec
    entryBuffer.putLong(if(lifetime < 0) -1 else now + lifetime) // expires at as milli sec
    entryBuffer.putShort(0.toShort) // errors
    entryBuffer.putInt(dataLength) // data length
    entryBuffer.put(compression) // compression
    entryBuffer.flip()
    assert(entryBuffer.limit() == JournaledFile.ENTRY_SIZE)
    writeDataWithEntry(entryOffset, dataBuffer, entryBuffer)
  }

  /**
    * 指定されたデータとエントリをジャーナル内の任意の位置に書き込みます。
    *
    * @param entryOffset 書き込む位置の先頭オフセット
    * @param dataBuffer  書き込むデータのバッファ
    * @param entryBuffer 書き込むエントリのバッファ
    * @return 書き込んだデータのエントリオフセット
    */
  private def writeDataWithEntry(entryOffset:Long, dataBuffer:ByteBuffer, entryBuffer:ByteBuffer):Long = {
    assert(entryBuffer.position() == 0 && entryBuffer.limit() == JournaledFile.ENTRY_SIZE)
    assert(dataBuffer.position() == 0)
    fc.write(entryBuffer, entryOffset)
    fc.write(dataBuffer, entryOffset + JournaledFile.ENTRY_SIZE)
    entryOffset
  }

  /**
    * 指定された位置からデータとエントリを読み込みます。
    *
    * @param entryOffset 読み込み位置の先頭オフセット
    * @param entryBuffer 読み込むエントリのバッファ (再利用目的のリターンバッファ)
    * @return 読み込んだデータのバッファ
    */
  private def readDataWithEntry(entryOffset:Long, entryBuffer:ByteBuffer):ByteBuffer = {
    readEntry(entryOffset, entryBuffer)
    readData(entryOffset, entryBuffer.getInt(offset.DATA_LENGTH))
  }

  /**
    * 指定された位置からエントリを読み込みます。
    *
    * @param entryOffset 読み込み位置の先頭オフセット
    * @param entryBuffer 読み込むエントリのバッファ (再利用目的のリターンバッファ)
    * @return 読み込んだエントリバッファ (`entryBuffer` と同一)
    */
  private def readEntry(entryOffset:Long, entryBuffer:ByteBuffer):ByteBuffer = {
    entryBuffer.clear()
    assert(entryBuffer.limit() == JournaledFile.ENTRY_SIZE)
    fc.read(entryBuffer, entryOffset)
    entryBuffer.flip()
    entryBuffer
  }

  /**
    * 指定された位置からデータを読み込みます。
    *
    * @param entryOffset 読み込み位置の先頭オフセット
    * @param dataLength  データの長さ
    * @return 読み込んだデータのバッファ
    */
  private def readData(entryOffset:Long, dataLength:Int):ByteBuffer = {
    val dataBuffer = ByteBuffer.allocate(dataLength)
    fc.read(dataBuffer, entryOffset + JournaledFile.ENTRY_SIZE)
    dataBuffer.flip()
    dataBuffer
  }

  /**
    * このジャーナルの内容を指定されたキューの末尾 (最も深い位置) に移動します。
    * 移動はこのジャーナルから FILO で取り出された順序で `queue` の最も深い位置に移動され、
    * `queue` 側に
    * この操作が成功するとジャーナルの内容は破棄されます。
    *
    * @param queue データ移動先のキュー
    */
  def migrateTo(queue:JournaledFile):Unit = {
    // キュー側に空ける必要のある領域サイズを計算
    val (entryCount, totalDataLength, _) = inspect()
    if(entryCount > 0) {
      val minimumSize = totalDataLength + entryCount * JournaledFile.ENTRY_SIZE
      val (_, _, maxDataLength) = queue.inspect()
      val spaceOutSize = math.max(maxDataLength + JournaledFile.ENTRY_SIZE, minimumSize)
      val deepestEntryOffset = queue.spaceOut(spaceOutSize)

      // このジャーナルの要素をキューへ移動
      val (dstLastNewEntryOffset, _) =
        aggregate((-1L, queue.header.size.toLong)) { case (entryBuffer, srcEntryOffset, (dstPrevEntryOffset, dstRegionOffset)) =>
          val dataLength = entryBuffer.getInt(offset.DATA_LENGTH)
          val dataBuffer = readData(srcEntryOffset, dataLength)
          entryBuffer.putLong(offset.PREVIOUS, dstPrevEntryOffset)
          val dstEntryOffset = queue.writeDataWithEntry(dstRegionOffset, dataBuffer, entryBuffer)
          (dstEntryOffset, dstRegionOffset + JournaledFile.ENTRY_SIZE + dataLength)
        }

      // メタ情報を更新して新しいエントリを連結
      if(deepestEntryOffset >= 0) {
        queue.raw.writeLong(deepestEntryOffset + offset.PREVIOUS, dstLastNewEntryOffset)
      } else {
        queue.header.lastPosition = dstLastNewEntryOffset
      }
      queue.header.currentItems = header.currentItems + entryCount
      queue.fc.force(true)

      // このジャーナルの内容を破棄
      fc.truncate(0) // ファイルオープン中に delete() できないケースを想定して 0 バイトに削除
      file.delete()
    }
  }

  /**
    * このジャーナルに含まれているエントリを走査し、エントリの個数、合計データサイズ、最大データサイズを参照します。
    * 合計データサイズ及び最大データサイズにエントリ領域の大きさは含まれません。
    *
    * @return (エントリ数, 合計データサイズ, 最大データサイズ)
    */
  private def inspect():(Long, Long, Int) = aggregate((0L, 0L, 0)) { case (entryBuffer, _, result) =>
    val dataLength = entryBuffer.getInt(offset.DATA_LENGTH)
    (result._1 + 1, result._2 + dataLength, math.max(result._3, dataLength))
  }

  /**
    * このジャーナルの先頭部分に指定されたサイズの空間を開けます。
    *
    * @param size 空ける空間のサイズ (バイト)
    * @return 最も深い位置のエントリのオフセット、またはエントリが存在しない場合は負の値
    */
  private def spaceOut(size:Long):Long = {

    val (pointer, _) = aggregate((offset.LAST_POSITION.toLong, fc.size() + size)) { case (entryBuffer, entryOffset, (prevPointerOffset, entryTailOffset)) =>

      // エントリの移動でエントリ領域がオーバーラップしないことを確認
      val newEntryOffset = entryTailOffset - JournaledFile.ENTRY_SIZE
      if(entryOffset + JournaledFile.ENTRY_SIZE > newEntryOffset) {
        throw new IllegalArgumentException(
          f"new entry region is overlapped; space-out size $size%,d must be larger than entry size ${JournaledFile.ENTRY_SIZE}%,d")
      }

      // エントリの読み込み
      entryBuffer.clear()
      fc.read(entryBuffer, entryOffset)
      entryBuffer.flip()
      val dataOffset = entryOffset + JournaledFile.ENTRY_SIZE

      // データの移動でデータ領域がオーバーラップしないことを確認
      val dataLength = entryBuffer.getInt(offset.DATA_LENGTH)
      val newDataOffset = newEntryOffset - dataLength
      if(dataOffset - dataLength > newDataOffset) {
        throw new IllegalArgumentException(
          f"new data region is overlapped; space-out size $size%,d must be larger than data size $dataLength%,d")
      }

      // エントリと新しいエントリ位置の書き込み (データの移動により潰れる可能性があるため先に書き込む必要がある)
      fc.write(entryBuffer, newEntryOffset)
      raw.writeLong(prevPointerOffset, newEntryOffset)

      // データの移動
      val dataBuffer = ByteBuffer.allocate(dataLength)
      val readDataLength = fc.read(dataBuffer, dataOffset)
      dataBuffer.flip()
      assert(readDataLength == dataLength)
      fc.write(dataBuffer, newDataOffset)

      (newEntryOffset + offset.PREVIOUS, newDataOffset)
    }

    if(pointer == offset.LAST_POSITION) -1 else pointer - offset.PREVIOUS
  }

  /**
    * このジャーナルに含まれている全てのエントリに対して繰り返し処理を行います。
    *
    * @param zero 集約のゼロ値
    * @param f    各エントリに対して適用する関数 (エントリ, エントリのオフセット, 前の結果)
    * @tparam T 集約値の型
    * @return 集約結果
    */
  private def aggregate[T](zero:T)(f:(ByteBuffer, Long, T) => T):T = {
    val entryBuffer = ByteBuffer.allocate(JournaledFile.ENTRY_SIZE)

    def _aggregate(entryOffset:Long, value:T):T = if(entryOffset < 0) value else {
      entryBuffer.clear()
      val readLength = fc.read(entryBuffer, entryOffset)
      assert(readLength == JournaledFile.ENTRY_SIZE)
      entryBuffer.flip()
      val prevEntryOffset = entryBuffer.getLong(offset.PREVIOUS)
      val newValue = f(entryBuffer, entryOffset, value)
      _aggregate(prevEntryOffset, newValue)
    }

    val longBuffer = ByteBuffer.allocate(java.lang.Long.BYTES)
    fc.read(longBuffer, offset.LAST_POSITION)
    _aggregate(longBuffer.getLong, zero)
  }

  /**
    * このファイルが Quebic Journal ファイルフォーマットであることを確認します。フォーマットが不正な場合は例外が発生し
    * ます。
    *
    * @throws FormatException フォーマットが不正な場合
    */
  @throws[FormatException]
  private[this] def validate():Unit = {

    // ヘッダサイズの確認
    if(fc.size() < header.size) {
      throw new FormatException(f"${if(fifo) "queue" else "journal"} file is too short, actual ${fc.size()},dB < expected ${header.size}%,dB")
    }

    val buffer = ByteBuffer.allocate(header.size)
    fc.read(buffer)

    // マジックナンバーの確認
    val magicNumber = buffer.getShort(offset.MAGIC_NUMBER)
    if(magicNumber != JournaledFile.MagicNumber) {
      throw new FormatException(f"invalid magic number, actual 0x${magicNumber & 0xFFFF}%04X != expected 0x${JournaledFile.MagicNumber}%04X")
    }

    // スキーマの確認
    buffer.position(offset.SCHEMA)
    val existing = Schema(buffer)
    if(schema.types.length != existing.types.length || !schema.types.zip(existing.types).forall(x => x._1.id == x._2.id)) {
      throw new FormatException(s"incompatible schema: actual $schema != expected $existing")
    }
  }

  override def close():Unit = {
    lock.release()
    fc.close()
  }
}

private[quebic] object JournaledFile {
  val MagicNumber:Short = ((('Q' & 0xFF) << 8) | ('B' & 0xFF)).toShort

  object offset {
    val MAGIC_NUMBER:Int = 0
    val HEADER_SIZE:Int = MAGIC_NUMBER + java.lang.Short.BYTES
    val CURRENT_ITEMS:Int = HEADER_SIZE + java.lang.Short.BYTES
    val LAST_POSITION:Int = CURRENT_ITEMS + java.lang.Long.BYTES
    val SCHEMA:Int = LAST_POSITION + java.lang.Long.BYTES

    val PREVIOUS:Int = 0
    val CREATED_AT:Int = PREVIOUS + java.lang.Long.BYTES
    val EXPIRES_AT:Int = CREATED_AT + java.lang.Long.BYTES
    val ERRORS:Int = EXPIRES_AT + java.lang.Long.BYTES
    val DATA_LENGTH:Int = ERRORS + java.lang.Short.BYTES
    val COMPRESSION:Int = DATA_LENGTH + Integer.BYTES
  }

  val ENTRY_SIZE:Int = offset.COMPRESSION + java.lang.Byte.BYTES

}