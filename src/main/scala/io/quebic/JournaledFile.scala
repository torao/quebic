package io.quebic

import java.io.{File, PrintWriter, StringWriter}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.text.SimpleDateFormat

import io.quebic.JournaledFile.{logger, offset}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec

private[quebic] class JournaledFile(file:File, schema:Schema) extends AutoCloseable {
  private[this] val path = file.toPath
  private val fc = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
  private[this] val lock = fc.lock()

  private[this] lazy val schemaBinary = schema.toByteArray

  locally {
    if(fc.size() == 0) {
      init()
    } else {
      validate()
    }
  }

  private object raw {
    private[this] val longBuffer = ByteBuffer.allocate(java.lang.Long.BYTES)

    /** ファイルの指定されたオフセットから Long 値を読み込みます。 */
    def readLong(offset:Long):Long = {
      longBuffer.clear()
      fc.read(longBuffer, offset)
      longBuffer.flip()
      longBuffer.getLong
    }

    /** ファイルの指定されたオフセットに Long 値を書き込みます。 */
    def writeLong(offset:Long, value:Long):Unit = {
      longBuffer.clear()
      longBuffer.putLong(value)
      longBuffer.flip()
      fc.write(longBuffer, offset)
    }
  }

  private object header {

    /** このジャーナルのヘッダサイズを参照します。 */
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

  /**
    * このジャーナルに含まれているデータ数を参照します。この値はキャッシュされたものであり、プロセスの強制停止などに
    * よって実際に参照可能なデータ数と異なる可能性があります。ただし、ジャーナル内にデータが存在するかを確認するために
    * 0 か 0 以外かは正確な評価を行うことができます。
    *
    * @return このジャーナルに含まれているデータ数
    */
  def size:Long = {
    val size = header.currentItems
    val fileSize = fc.size()
    val correctSize = if(size == 0 && fileSize > header.size) {
      val (entryCount, _, _) = inspect()
      entryCount
    } else if(fileSize == header.size && size > 0) {
      0L
    } else size
    if(correctSize != size) {
      logger.debug(f"correcting the number of items: $size%,d -> $correctSize%,d")
      header.currentItems = correctSize
    }
    correctSize
  }

  /**
    * 指定されたデータをこのジャーナルに追加します。
    *
    * データの有効期限は現在時刻からの相対時間をミリ秒で指定します。この時刻を過ぎてもキューから取り出されなかった
    * データは破棄されます。データが有効期限を持たない場合は負の値を指定します。
    *
    * @param values      ジャーナルに追加するデータ
    * @param lifetime    データの有効期限を示す現在時刻からの相対時間 (ミリ秒)。負の値を指定した場合は有効期限なし
    * @param compression データの圧縮方法
    */
  def push(values:Struct, lifetime:Long = -1, compression:CompressionType = CompressionType.NONE):Unit = {

    // 現在のエントリ数を取得
    val currentItems = header.currentItems

    // データとエントリの書き込み
    val serializedValues = schema.serialize(values, compression)
    val previous = header.lastPosition
    val regionOffset = fc.size()
    val entryOffset = writeDataWithEntry(regionOffset, serializedValues, previous, lifetime, compression.id)

    // ヘッダの更新
    header.currentItems = currentItems + 1
    header.lastPosition = entryOffset
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
    consumeEntryWithData(errorPermitCount) { case (entryBuffer, dataBuffer) =>
      val data = try {
        schema.deserialize(dataBuffer)
      } catch {
        case ex:Exception =>
          val ent = JournaledFile.dumpEntry(entryBuffer)
          val bin = (0 until dataBuffer.limit()).map(i => f"${dataBuffer.get(i)}%02X").mkString
          throw new FormatException(s"deserialization failed: schema=$schema, entry=[$ent], buffer=$bin", ex)
      }
      f(data)
    }
  }

  /**
    * このジャーナルの最も新しく追加されたデータを参照し `f` に渡します。`f` が正常に終了した場合、そのデータはジャーナル
    * から取り除かれます。`f` で例外が発生した場合、そのデータにはエラーが加算され、必要に応じて廃棄されます。
    *
    * @param maxRetry データに対するエラーの許容回数
    * @param f        データに対して適用する処理
    * @tparam T 処理結果の型
    * @return 処理時のエラーは `Left[Throwable]`、取り出すデータが存在しない場合 `Right(None)`、データが存在し処理が
    *         正常に終了した場合 `Right(Some(result))`
    */
  @tailrec
  private[this] def consumeEntryWithData[T](maxRetry:Short)(f:(ByteBuffer, ByteBuffer) => T):Either[Throwable, Option[T]] = {

    // 末尾のエントリ位置を読み込み
    val entryOffset = header.lastPosition
    if(entryOffset < 0) {
      Right(None)
    } else {
      assert(entryOffset >= header.size, f"invalid entry offset: $entryOffset%,d < ${header.size}%,d")

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
        result
      } else {
        // 失敗している場合はエントリを更新
        entryBuffer.position(0)
        assert(entryBuffer.limit() == JournaledFile.ENTRY_SIZE)
        fc.write(entryBuffer, entryOffset)
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
    entryBuffer.put(JournaledFile.EntrySignature)
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
    val len = fc.read(entryBuffer, entryOffset)
    if(len < JournaledFile.ENTRY_SIZE) {
      throw new FormatException(f"entry too short: $len%,dB < ${JournaledFile.ENTRY_SIZE}%,d, offset=0x$entryOffset%X")
    }
    entryBuffer.flip()
    if(entryBuffer.get(offset.ENTRY_SIG) != JournaledFile.EntrySignature) {
      throw new FormatException(f"invalid entry signature: ${entryBuffer.get(offset.ENTRY_SIG) & 0xFF}%02X != ${JournaledFile.EntrySignature}%02X, offset=0x$entryOffset%X")
    }
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
    val dataOffset = entryOffset + JournaledFile.ENTRY_SIZE
    val len = fc.read(dataBuffer, dataOffset)
    if(len < 0) {
      throw new FormatException(f"data region over-run journal: data-offset data-offset=0x$dataOffset%X, data-length=$dataLength%,dB, file-size=${fc.size()}%X, entry-offset=0x$entryOffset%X")
    }
    if(len < dataLength) {
      throw new FormatException(f"data region too short: $len%,dB < $dataLength%,d, offset=0x$entryOffset%X")
    }
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
      val t0 = System.currentTimeMillis()

      val minimumSize = totalDataLength + entryCount * JournaledFile.ENTRY_SIZE
      val (_, _, maxDataLength) = queue.inspect()
      val spaceOutSize = math.max(maxDataLength + JournaledFile.ENTRY_SIZE, minimumSize)
      val deepestEntryOffset = queue.spaceOut(spaceOutSize)

      // このジャーナルの要素をキューへ移動
      val (dstLastNewEntryOffset, _) = aggregate((-1L, queue.header.size.toLong)) { case (entryBuffer, srcEntryOffset, (dstPrevEntryOffset, dstRegionOffset)) =>
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
      queue.header.currentItems = queue.header.currentItems + entryCount

      // このジャーナルの内容を破棄
      fc.truncate(0) // ファイルオープン中に delete() できないケースを想定して 0 バイトに削除
      file.delete()

      if(logger.isDebugEnabled) {
        val t = System.currentTimeMillis() - t0
        logger.debug(f"${file.getName}%s migrated: $entryCount%,d entries ($totalDataLength%,dB) move to queue, total ${queue.size}%,d entries, $t%,d[ms]")
      }
    }
  }

  def verify():Unit = aggregate(fc.size()) { case (entryBuffer, entryOffset, rightEntryOffset) =>
    val dataLength = entryBuffer.getInt(offset.DATA_LENGTH)
    if(dataLength <= 0) {
      val ent = JournaledFile.dumpEntry(entryBuffer)
      throw new FormatException(f"invalid data length: [0x$entryOffset%X] $ent")
    }
    if(entryOffset + JournaledFile.ENTRY_SIZE + dataLength > rightEntryOffset) {
      throw new FormatException(f"entry over-run: 0x$entryOffset%X + ${JournaledFile.ENTRY_SIZE}%,dB + $dataLength%,dB = 0x${entryOffset + JournaledFile.ENTRY_SIZE + dataLength}%X > right 0x$rightEntryOffset%X")
    }
    entryOffset
  }

  /**
    * このジャーナルに含まれているエントリを走査し、エントリの個数、合計データサイズ、最大データサイズを参照します。
    * 合計データサイズ及び最大データサイズにエントリ領域の大きさは含まれません。
    *
    * @return (エントリ数, 合計データサイズ, 最大データサイズ)
    */
  private def inspect():(Long, Long, Int) = {
    var entryCount = 0L
    var totalDataLength = 0L
    var maxDataLength = 0
    foreach { case (entryBuffer, _) =>
      val dataLength = entryBuffer.getInt(offset.DATA_LENGTH)
      entryCount += 1
      totalDataLength += dataLength
      maxDataLength = math.max(maxDataLength, dataLength)
    }
    (entryCount, totalDataLength, maxDataLength)
  }

  /**
    * このジャーナルの先頭部分に指定されたサイズの空間を開けます。
    *
    * @param size 空ける空間のサイズ (バイト)
    * @return 最も深い位置のエントリのオフセット、またはエントリが存在しない場合は負の値
    */
  private def spaceOut(size:Long):Long = {

    val (pointer, _) = aggregate((offset.LAST_POSITION.toLong, fc.size() + size)) { case (entryBuffer, entryOffset, (prevPointerOffset, entryTailOffset)) =>

      // エントリとデータの移動で領域がオーバーラップしないことを確認
      val dataLength = entryBuffer.getInt(offset.DATA_LENGTH)
      val newEntryOffset = entryTailOffset - JournaledFile.ENTRY_SIZE - dataLength
      if(entryOffset + JournaledFile.ENTRY_SIZE + dataLength > newEntryOffset) {
        throw new IllegalArgumentException(
          f"new entry region is overlapped; space-out size $size%,d must be larger than entry size ${JournaledFile.ENTRY_SIZE + dataLength}%,d bytes")
      }

      // データの移動
      val dataOffset = entryOffset + JournaledFile.ENTRY_SIZE
      val newDataOffset = newEntryOffset + JournaledFile.ENTRY_SIZE
      val dataBuffer = ByteBuffer.allocate(dataLength)
      val readDataLength = fc.read(dataBuffer, dataOffset)
      dataBuffer.flip()
      assert(readDataLength == dataLength)
      fc.write(dataBuffer, newDataOffset)

      // エントリと新しいエントリ位置の書き込み
      fc.write(entryBuffer, newEntryOffset)
      raw.writeLong(prevPointerOffset, newEntryOffset)

      (newEntryOffset + offset.PREVIOUS, newEntryOffset)
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

    _aggregate(header.lastPosition, zero)
  }

  /**
    * このジャーナルに含まれている全てのエントリに対して繰り返し処理を行います。
    *
    * @param f 各エントリに対して適用する関数 (エントリ, エントリのオフセット)
    */
  private def foreach(f:(ByteBuffer, Long) => Unit):Unit = aggregate(()) { case (entryBuffer, entryOffset, _) =>
    f(entryBuffer, entryOffset)
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
      throw new FormatException(f"journal file is too short, actual ${fc.size()},dB < expected ${header.size}%,dB")
    }

    val buffer = ByteBuffer.allocate(header.size)
    fc.read(buffer)

    // マジックナンバーの確認
    val magicNumber = buffer.getShort(offset.MAGIC_NUMBER)
    if(magicNumber != JournaledFile.MagicNumber) {
      throw new FormatException(f"invalid magic number, actual 0x${magicNumber & 0xFFFF}%04X != expected 0x${JournaledFile.MagicNumber}%04X")
    }

    // スキーマの互換性確認
    buffer.position(offset.SCHEMA)
    val existing = Schema(buffer)
    if(schema.types.length != existing.types.length || !schema.types.zip(existing.types).forall(x => x._1.id == x._2.id)) {
      throw new FormatException(s"incompatible schema: actual $schema != expected $existing")
    }
  }

  def dump():String = {
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    pw.println(toString)
    val df = new SimpleDateFormat("yyyyMMddHHmmss.SSS")
    var entryCount = 0L
    var totalDataLength = 0L
    var maxDataLength = 0
    foreach { case (entryBuffer, entryOffset) =>
      val entrySignature = entryBuffer.get(offset.ENTRY_SIG)
      val previous = entryBuffer.getLong(offset.PREVIOUS)
      val createdAt = df.format(entryBuffer.getLong(offset.CREATED_AT))
      val expiresAt = entryBuffer.getLong(offset.EXPIRES_AT)
      val expiresAtStr = if(expiresAt <= 0) "" else " " + df.format(entryBuffer.getLong(offset.EXPIRES_AT)) + " expr, "
      val errors = entryBuffer.getShort(offset.ERRORS) & 0xFFFF
      val dataLength = entryBuffer.getInt(offset.DATA_LENGTH)
      val compression = CompressionType.valueOf(entryBuffer.get(offset.COMPRESSION))
      pw.println(f"  0x$entrySignature%02X 0x$entryOffset%08X, next 0x$previous%08X, $createdAt add, $expiresAtStr$errors%,d err, $dataLength%,dB data, ${compression.name} compressed")
      entryCount += 1
      totalDataLength += dataLength
      maxDataLength = math.max(maxDataLength, dataLength)
    }
    pw.println(f"$entryCount%,d entries, total $totalDataLength%,dB data, max $maxDataLength%,dB data")
    pw.flush()
    sw.toString
  }

  override def toString:String = if(file.length() > 0) {
    val headerBuffer = ByteBuffer.allocate(header.size)
    fc.read(headerBuffer, offset.MAGIC_NUMBER)
    headerBuffer.flip()
    val magicNumber = headerBuffer.getShort(offset.MAGIC_NUMBER) & 0xFFFF
    val headerSize = headerBuffer.getShort(offset.HEADER_SIZE) & 0xFFFF
    val currentItems = headerBuffer.getLong(offset.CURRENT_ITEMS)
    val lastPosition = headerBuffer.getLong(offset.LAST_POSITION)
    headerBuffer.position(offset.SCHEMA)
    val schema = Schema(headerBuffer)
    f"$schema 0x$magicNumber%04X $headerSize%,dB $currentItems%,d items, 0x$lastPosition%X"
  } else {
    f"$schema 0x${JournaledFile.MagicNumber}%04X ${header.size}%,dB ${0}%,d items, 0x${header.size}%X"
  }

  override def close():Unit = {
    lock.release()
    fc.close()
  }
}

private[quebic] object JournaledFile {
  private[JournaledFile] val logger = LoggerFactory.getLogger(classOf[JournaledFile])
  val MagicNumber:Short = ((('Q' & 0xFF) << 8) | ('B' & 0xFF)).toShort
  val EntrySignature:Byte = '@'

  def dumpEntry(entryBuffer:ByteBuffer):String = {
    val sig = entryBuffer.get(offset.ENTRY_SIG)
    val prev = entryBuffer.getLong(offset.PREVIOUS)
    val tm = entryBuffer.getLong(offset.CREATED_AT)
    val err = entryBuffer.getShort(offset.ERRORS) & 0xFFFF
    val len = entryBuffer.getInt(offset.DATA_LENGTH)
    val cmp = entryBuffer.get(offset.COMPRESSION) & 0xFF
    f"sig=0x$sig%02X, prev=0x$prev%X, created=$tm%tF $tm%tT, errors=$err%,d, data-length=$len%,dB, compress=$cmp%d"
  }

  object offset {
    val MAGIC_NUMBER:Int = 0
    val HEADER_SIZE:Int = MAGIC_NUMBER + java.lang.Short.BYTES
    val CURRENT_ITEMS:Int = HEADER_SIZE + java.lang.Short.BYTES
    val LAST_POSITION:Int = CURRENT_ITEMS + java.lang.Long.BYTES
    val SCHEMA:Int = LAST_POSITION + java.lang.Long.BYTES

    val ENTRY_SIG:Int = 0
    val PREVIOUS:Int = ENTRY_SIG + java.lang.Byte.BYTES
    val CREATED_AT:Int = PREVIOUS + java.lang.Long.BYTES
    val EXPIRES_AT:Int = CREATED_AT + java.lang.Long.BYTES
    val ERRORS:Int = EXPIRES_AT + java.lang.Long.BYTES
    val DATA_LENGTH:Int = ERRORS + java.lang.Short.BYTES
    val COMPRESSION:Int = DATA_LENGTH + Integer.BYTES
  }

  val ENTRY_SIZE:Int = offset.COMPRESSION + java.lang.Byte.BYTES

}