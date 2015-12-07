package chana.serializer

import akka.actor.ExtendedActorSystem
import akka.serialization.{ SerializationExtension, Serializer }
import akka.util.ByteString
import chana.avro.UpdateEvent
import chana.avro.Binlog
import java.nio.ByteOrder

final class UpdateEventSerializer(system: ExtendedActorSystem) extends Serializer {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  override def identifier: Int = 302668162

  override def includeManifest: Boolean = false

  private lazy val binlogSerializer = SerializationExtension(system).serializerFor(classOf[Binlog])

  override def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case UpdateEvent(binlogs) =>
      val builder = ByteString.newBuilder

      val len = binlogs.length
      builder.putInt(len)
      var i = 0
      while (i < len) {
        val binlog = binlogs(i)
        val payload = binlogSerializer.toBinary(binlog)
        builder.putInt(payload.length)
        builder.putBytes(payload)
        i += 1
      }
      builder.result.toArray

    case _ =>
      throw new IllegalArgumentException("Can't serialize a non-Avro message using AvroSerializer [" + obj + "]")
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val data = ByteString(bytes).iterator

    val len = data.getInt
    val binlogs = Array.ofDim[Binlog](len)
    var i = 0
    while (i < len) {
      val size = data.getInt
      val payload = Array.ofDim[Byte](size)
      data.getBytes(payload)
      binlogs(i) = binlogSerializer.fromBinary(payload).asInstanceOf[Binlog]
      i += 1
    }
    UpdateEvent(binlogs)
  }

}
