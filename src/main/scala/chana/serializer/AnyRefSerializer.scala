package chana.serializer

import akka.serialization.Serialization
import akka.util.ByteIterator
import akka.util.ByteStringBuilder
import java.nio.ByteOrder

object AnyRefSerializer {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  def fromAnyRef(serialization: Serialization, builder: ByteStringBuilder, value: AnyRef): Unit = {
    val valueSerializer = serialization.findSerializerFor(value)
    builder.putInt(valueSerializer.identifier)
    val payload = valueSerializer.toBinary(value)
    builder.putInt(payload.length)
    builder.putBytes(payload)
  }

  def toAnyRef(serialization: Serialization, data: ByteIterator): AnyRef = {
    val serializerId = data.getInt
    val size = data.getInt
    val payload = Array.ofDim[Byte](size)
    data.getBytes(payload)
    serialization.serializerByIdentity.get(serializerId) match {
      case Some(valueSerializer) => valueSerializer.fromBinary(payload)
      case None                  => null
    }
  }
}
