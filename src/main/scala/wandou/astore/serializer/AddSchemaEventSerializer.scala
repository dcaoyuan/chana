package wandou.astore.serializer

import java.nio.ByteOrder

import akka.actor.ExtendedActorSystem
import akka.serialization.{ SerializationExtension, Serializer }
import akka.util.{ ByteIterator, ByteString, ByteStringBuilder }
import org.apache.avro.Schema
import wandou.astore.AddSchema

import scala.concurrent.duration.Duration

class AddSchemaEventSerializer(val system: ExtendedActorSystem) extends Serializer {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  override def identifier: Int = 238710283

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
    case schemaEvent: AddSchema =>
      val builder = ByteString.newBuilder
      fromAnyRef(builder, schemaEvent.entityName.asInstanceOf[AnyRef])
      fromAnyRef(builder, schemaEvent.schema.asInstanceOf[AnyRef])
      fromAnyRef(builder, schemaEvent.idleTimeout.asInstanceOf[AnyRef])
      builder.result.toArray

    case _ => {
      val errorMsg = "Can't serialize a non-Schema message using SchemaSerializer [" + obj + "]"
      throw new IllegalArgumentException(errorMsg)
    }
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val data = ByteString(bytes).iterator
    AddSchema(toAnyRef(data).asInstanceOf[String], toAnyRef(data).asInstanceOf[Schema], toAnyRef(data).asInstanceOf[Duration])
  }
}
