package chana.serializer

import akka.actor.ExtendedActorSystem
import akka.serialization.{ SerializationExtension, Serializer }
import akka.util.ByteString
import chana.UpdatedFields
import java.nio.ByteOrder

final class RecordEventSerializer(system: ExtendedActorSystem) extends Serializer {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  override def identifier: Int = 302668162

  override def includeManifest: Boolean = false

  private lazy val serialization = SerializationExtension(system)

  override def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case UpdatedFields(posToField) =>
      val builder = ByteString.newBuilder

      val size = posToField.size
      builder.putInt(size)
      var i = 0
      while (i < size) {
        val entry = posToField(i)
        builder.putInt(entry._1)
        AnyRefSerializer.fromAnyRef(serialization, builder, entry._2.asInstanceOf[AnyRef])
        i += 1
      }
      builder.result.toArray

    case _ =>
      throw new IllegalArgumentException("Can't serialize a non-Avro message using AvroSerializer [" + obj + "]")
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val data = ByteString(bytes).iterator

    val size = data.getInt
    val result = Array.ofDim[(Int, Any)](size)
    var i = 0
    while (i < size) {
      result(i) = (data.getInt, AnyRefSerializer.toAnyRef(serialization, data))
      i += 1
    }
    UpdatedFields(result.toList)
  }

}
