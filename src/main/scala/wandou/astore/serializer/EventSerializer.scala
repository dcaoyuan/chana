package wandou.astore.serializer

import java.nio.ByteOrder

import akka.actor.ExtendedActorSystem
import akka.serialization.{ SerializationExtension, Serializer }
import akka.util.{ ByteIterator, ByteString, ByteStringBuilder }
import wandou.astore.UpdatedFields

class EventSerializer(val system: ExtendedActorSystem) extends Serializer {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  override def identifier: Int = 302668162

  override def includeManifest: Boolean = true

  private lazy val serialization = SerializationExtension(system)

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

  override def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case record: UpdatedFields =>
      val builder = ByteString.newBuilder

      val size = record.updatedFields.size
      builder.putInt(size)
      0 until size foreach {
        i =>
          builder.putInt(record.updatedFields(i)._1)
          fromAnyRef(builder, record.updatedFields(i)._2.asInstanceOf[AnyRef])
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
    val result = Array.ofDim[(Int, Any)](size)
    0 until size foreach {
      i =>
        result(i) = (data.getInt, toAnyRef(data))
    }
    UpdatedFields(result.toList)
  }

}
