package akka.persistence.serialization

import java.nio.ByteOrder

import akka.actor.ExtendedActorSystem
import akka.persistence.PersistentRepr
import akka.persistence.journal.AsyncWriteTarget.WriteMessages
import akka.serialization.{ SerializationExtension, Serializer }
import akka.util.{ ByteIterator, ByteString, ByteStringBuilder }
import wandou.astore.serializer.StringSerializer

class WriteMessagesSerializer(val system: ExtendedActorSystem) extends Serializer {

  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  override def identifier: Int = 629565034

  override def includeManifest: Boolean = true

  private lazy val serialization = SerializationExtension(system)

  val WriteMessagesClass = classOf[WriteMessages]

  final def fromAnyRef(builder: ByteStringBuilder, value: AnyRef): Unit = {
    val valueSerializer = serialization.findSerializerFor(value)
    StringSerializer.appendToBuilder(builder, valueSerializer.getClass.getName)
    val bin = valueSerializer.toBinary(value)
    builder.putInt(bin.length)
    builder.putBytes(bin)
  }

  final def toAnyRef(data: ByteIterator): AnyRef = {
    val clazz = StringSerializer.fromByteIterator(data)
    val payloadSize = data.getInt
    val payload = Array.ofDim[Byte](payloadSize)
    data.getBytes(payload)
    val valueSerializer = serialization.serializerOf(clazz)
    val value = if (valueSerializer.isSuccess) {
      valueSerializer.get.fromBinary(payload)
    } else null
    value
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = manifest match {
    case Some(c) =>
      c match {
        case WriteMessagesClass =>
          val data = ByteString(bytes).iterator
          val size = data.getInt
          val messages = Array.ofDim[PersistentRepr](size)
          0 until size foreach (i => {
            messages(i) = toAnyRef(data).asInstanceOf[PersistentRepr]
          })
          WriteMessages(messages.to[collection.immutable.Seq])
        case _ => throw new IllegalArgumentException(s"Can't deserialize object of type ${c}")
      }

    case _ => throw new IllegalArgumentException(s"Can't deserialize object of type ${manifest}")

  }

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case w: WriteMessages =>
      val builder = ByteString.newBuilder
      val size = w.messages.size
      builder.putInt(size)
      w.messages.foreach(r => fromAnyRef(builder, r))
      builder.result.toArray
    case _ => throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass}")

  }

}
