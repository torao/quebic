package at.hazm.quebic

import org.specs2.Specification
import org.specs2.execute.Result
import org.specs2.specification.core.SpecStructure

import scala.util.Random

class CodecTest extends Specification {
  override def is:SpecStructure =
    s2"""
none codec: ${randomDecode(53, 1024, Codec.PLAIN)}
gzip codec: ${randomDecode(53, 1024, Codec.GZIP)}
"""

  def randomDecode(seed:Long, length:Int, codec:Codec):Result = {
    val random = new Random(seed)
    val binary1 = new Array[Byte](length)
    random.nextBytes(binary1)
    val encoded = codec.encode(binary1)
    val binary2 = codec.decode(encoded)
    System.out.println(f"${codec.name.toUpperCase}: ${binary1.length}%,dB -> ${encoded.length}%,dB")
    binary1 === binary2
  }

}
