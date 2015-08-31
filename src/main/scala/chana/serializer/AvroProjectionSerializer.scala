package chana.serializer

import akka.actor.ExtendedActorSystem
import akka.serialization.Serializer
import akka.util.ByteString
import chana.jpql.MapperProjection
import chana.jpql.VoidProjection
import java.nio.ByteOrder

final class AvroProjectionSerializer(system: ExtendedActorSystem) extends Serializer {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  override def identifier: Int = 124621389

  override def includeManifest: Boolean = false

  override def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case projection: MapperProjection =>
      val builder = ByteString.newBuilder
      StringSerializer.appendToByteString(builder, projection.id)
      builder.putBytes(projection.projection)
      builder.result.toArray
    case projection: VoidProjection =>
      val builder = ByteString.newBuilder
      StringSerializer.appendToByteString(builder, projection.id)
      builder.result.toArray

    case _ =>
      throw new IllegalArgumentException("Can't serialize a non-Avro message using AvroSerializer [" + obj + "]")
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val data = ByteString(bytes).iterator
    val id = StringSerializer.fromByteIterator(data)
    val len = data.len
    if (len > 0) {
      val payload = Array.ofDim[Byte](data.len)
      data.getBytes(payload)
      MapperProjection(id, payload)
    } else {
      VoidProjection(id)
    }
  }
}
