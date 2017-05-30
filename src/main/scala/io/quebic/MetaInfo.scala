package io.quebic

import java.nio.ByteBuffer

/**
  * キューのメタ情報を現すクラス。
  */
private[quebic] class MetaInfo(header:ByteBuffer) {
  if(header.remaining() < java.lang.Short.BYTES || header.getShort(0) != MetaInfo.MagicNumber){
    throw new FormatException(f"invalid magic number: 0x${header.getShort(0)}%04X")
  }
  if(header.remaining() < java.lang.Short.BYTES || header.getShort(0) != MetaInfo.MagicNumber){
    throw new FormatException(f"invalid magic number: 0x${header.getShort(0)}%04X")
  }


}

object MetaInfo {
  val MagicNumber:Short = (('Q' << 8) | 'B').toShort
}