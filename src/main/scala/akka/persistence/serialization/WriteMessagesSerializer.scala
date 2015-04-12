package akka.persistence.serialization

import akka.actor.ExtendedActorSystem
import akka.persistence.PersistentRepr
import akka.persistence.journal.AsyncWriteTarget.WriteMessages
import akka.serialization.{ SerializationExtension, Serializer }
import akka.util.ByteString
import java.nio.ByteOrder
import wandou.astore.serializer.AnyRefSerializer

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
      w.messages.foreach(r => AnyRefSerializer.fromAnyRef(serialization, builder, r))
      builder.result.toArray

    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass}")

  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val data = ByteString(bytes).iterator
    val size = data.getInt
    val messages = Array.ofDim[PersistentRepr](size)
    var i = 0
    while (i < size) {
      messages(i) = AnyRefSerializer.toAnyRef(serialization, data).asInstanceOf[PersistentRepr]
      i += 1
    }
    WriteMessages(messages.to[collection.immutable.Seq])
  }

}
