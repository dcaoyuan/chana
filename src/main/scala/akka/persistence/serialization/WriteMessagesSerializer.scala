package akka.persistence.serialization

import akka.actor.ExtendedActorSystem
import akka.persistence.AtomicWrite
import akka.persistence.PersistentRepr
import akka.persistence.journal.AsyncWriteTarget.WriteMessages
import akka.serialization.{ SerializationExtension, Serializer }
import akka.util.ByteString
import chana.serializer.AnyRefSerializer
import java.nio.ByteOrder
import scala.collection.mutable

/**
 * @see https://github.com/akka/akka/issues/16802
 */
final class WriteMessagesSerializer(system: ExtendedActorSystem) extends Serializer {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  override def identifier: Int = 629565034

  override def includeManifest: Boolean = false

  private lazy val serialization = SerializationExtension(system)

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case w: WriteMessages =>
      val builder = ByteString.newBuilder
      val size = w.messages.size
      builder.putInt(size)
      w.messages.foreach { a =>
        builder.putInt(a.payload.size)
        a.payload.foreach { p =>
          AnyRefSerializer.fromAnyRef(serialization, builder, p)
        }
      }
      builder.result.toArray

    case _ => throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass}")

  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val data = ByteString(bytes).iterator
    val size = data.getInt
    val messages = Array.ofDim[AtomicWrite](size)
    var i = 0
    while (i < size) {
      val l = data.getInt
      val payloads = Array.ofDim[PersistentRepr](l)
      var j = 0
      while (j < l) {
        payloads(j) = AnyRefSerializer.toAnyRef(serialization, data).asInstanceOf[PersistentRepr]
        j += 1
      }
      messages(i) = AtomicWrite(payloads.toList)
      i += 1
    }
    WriteMessages(messages.toList)
  }

}
