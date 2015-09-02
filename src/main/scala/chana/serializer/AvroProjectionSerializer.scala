package chana.serializer

import akka.actor.ExtendedActorSystem
import akka.serialization.Serializer
import akka.util.ByteString
import chana.jpql.BinaryProjection
import chana.jpql.RemoveProjection
import java.nio.ByteOrder

final class AvroProjectionSerializer(system: ExtendedActorSystem) extends Serializer {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  override def identifier: Int = 124621389

  override def includeManifest: Boolean = false

  override def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case BinaryProjection(id, projection) =>
      val builder = ByteString.newBuilder
      StringSerializer.appendToByteString(builder, id)
      builder.putBytes(projection)
      builder.result.toArray

    case RemoveProjection(id) =>
      val builder = ByteString.newBuilder
      StringSerializer.appendToByteString(builder, id)
      builder.result.toArray

    case _ =>
      throw new IllegalArgumentException("Can't serialize a non-projection message using AvroProjectionSerializer [" + obj + "]")
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val data = ByteString(bytes).iterator
    val id = StringSerializer.fromByteIterator(data)
    val len = data.len
    if (len > 0) {
      val payload = Array.ofDim[Byte](data.len)
      data.getBytes(payload)
      BinaryProjection(id, payload)
    } else {
      RemoveProjection(id)
    }
  }
}
