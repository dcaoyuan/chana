package akka.persistence.serialization

import akka.actor.ExtendedActorSystem
import akka.persistence.PersistentRepr
import akka.persistence.journal.AsyncWriteTarget.WriteMessages
import akka.serialization.{ SerializationExtension, Serializer }
import akka.util.{ ByteIterator, ByteString, ByteStringBuilder }
import java.nio.ByteOrder

final class WriteMessagesSerializer(system: ExtendedActorSystem) extends Serializer {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  override def identifier: Int = 629565034

  override def includeManifest: Boolean = false

  private lazy val serialization = SerializationExtension(system)

  def fromAnyRef(builder: ByteStringBuilder, value: AnyRef): Unit = {
    val valueSerializer = serialization.findSerializerFor(value)
    builder.putInt(valueSerializer.identifier)
    val bin = valueSerializer.toBinary(value)
    builder.putInt(bin.length)
    builder.putBytes(bin)
  }

  def toAnyRef(data: ByteIterator): AnyRef = {
    val serializerId = data.getInt
    val payloadSize = data.getInt
    val payload = Array.ofDim[Byte](payloadSize)
    data.getBytes(payload)
    serialization.serializerByIdentity.get(serializerId) match {
      case Some(valueSerializer) => valueSerializer.fromBinary(payload)
      case None                  => null
    }
  }

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case w: WriteMessages =>
      val builder = ByteString.newBuilder
      val size = w.messages.size
      builder.putInt(size)
      w.messages.foreach(r => fromAnyRef(builder, r))
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
      messages(i) = toAnyRef(data).asInstanceOf[PersistentRepr]
      i += 1
    }
    WriteMessages(messages.to[collection.immutable.Seq])
  }

}
