package chana.serializer

import akka.util.{ ByteIterator, ByteStringBuilder }
import java.nio.ByteOrder

object StringSerializer {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  def appendToByteString(builder: ByteStringBuilder, str: String) {
    if (str != null) {
      val bytes = str.getBytes
      builder.putInt(bytes.length)
      builder.putBytes(bytes)
    } else {
      builder.putInt(-1)
    }
  }

  def fromByteIterator(data: ByteIterator): String = {
    val len = data.getInt
    if (len >= 0) {
      val str = Array.ofDim[Byte](len)
      data.getBytes(str)
      new String(str)
    } else {
      null
    }
  }
}
