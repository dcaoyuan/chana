package wandou.astore.serializer

import akka.actor.ExtendedActorSystem
import akka.serialization.{ SerializationExtension, Serializer }
import akka.util.{ ByteIterator, ByteString, ByteStringBuilder }
import java.nio.ByteOrder
import wandou.astore.UpdatedFields

final class RecordEventSerializer(system: ExtendedActorSystem) extends Serializer {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  override def identifier: Int = 302668162

  override def includeManifest: Boolean = false

  private lazy val serialization = SerializationExtension(system)

  def fromAnyRef(builder: ByteStringBuilder, value: AnyRef): Unit = {
    val valueSerializer = serialization.findSerializerFor(value)
    builder.putInt(valueSerializer.identifier)
    val payload = valueSerializer.toBinary(value)
    builder.putInt(payload.length)
    builder.putBytes(payload)
  }

  def toAnyRef(data: ByteIterator): AnyRef = {
    val serializerId = data.getInt
    val size = data.getInt
    val payload = Array.ofDim[Byte](size)
    data.getBytes(payload)
    serialization.serializerByIdentity.get(serializerId) match {
      case Some(valueSerializer) => valueSerializer.fromBinary(payload)
      case None                  => null
    }
  }

  override def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case record: UpdatedFields =>
      val builder = ByteString.newBuilder

      val size = record.updatedFields.size
      builder.putInt(size)
      var i = 0
      while (i < size) {
        builder.putInt(record.updatedFields(i)._1)
        fromAnyRef(builder, record.updatedFields(i)._2.asInstanceOf[AnyRef])
        i += 1
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
    var i = 0
    while (i < size) {
      result(i) = (data.getInt, toAnyRef(data))
      i += 1
    }
    UpdatedFields(result.toList)
  }

}
