package chana.serializer

import akka.actor.ExtendedActorSystem
import akka.serialization.SerializationExtension
import akka.serialization.Serializer
import akka.util.ByteString
import java.nio.ByteOrder

final class JavaMapSerializer(system: ExtendedActorSystem) extends Serializer {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  override def identifier: Int = 302668172

  override def includeManifest: Boolean = false

  private lazy val serialization = SerializationExtension(system)

  override def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case map: java.util.Map[_, _] =>
      val builder = ByteString.newBuilder

      val size = map.size
      builder.putInt(size)
      val entries = map.entrySet.iterator
      while (entries.hasNext) {
        val entry = entries.next
        AnyRefSerializer.fromAnyRef(serialization, builder, entry.getKey.asInstanceOf[AnyRef])
        AnyRefSerializer.fromAnyRef(serialization, builder, entry.getValue.asInstanceOf[AnyRef])
      }
      builder.result.toArray

    case _ => {
      val errorMsg = "Can't serialize a non-Avro message using AvroSerializer [" + obj + "]"
      throw new IllegalArgumentException(errorMsg)
    }
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val data = ByteString(bytes).iterator

    val size = data.getInt
    val result = new java.util.HashMap[AnyRef, AnyRef]()
    var i = 0
    while (i < size) {
      result.put(AnyRefSerializer.toAnyRef(serialization, data), AnyRefSerializer.toAnyRef(serialization, data))
      i += 1
    }
    result
  }

}
