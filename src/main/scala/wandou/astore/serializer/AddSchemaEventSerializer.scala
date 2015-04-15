package wandou.astore.serializer

import akka.actor.ExtendedActorSystem
import akka.serialization.{ Serializer }
import akka.util.ByteString
import java.nio.ByteOrder
import java.util.concurrent.TimeUnit
import org.apache.avro.Schema
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import wandou.astore.AddSchema

final class AddSchemaEventSerializer(system: ExtendedActorSystem) extends Serializer {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  override def identifier: Int = 238710283

  override def includeManifest: Boolean = false

  override def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case AddSchema(entityName, schema, idleTimeout) =>
      val builder = ByteString.newBuilder
      StringSerializer.appendToByteString(builder, entityName)
      StringSerializer.appendToByteString(builder, schema.toString)
      if (idleTimeout.isFinite) {
        builder.putLong(idleTimeout.toMillis)
      } else {
        builder.putLong(-1)
      }
      builder.result.toArray

    case _ => {
      val errorMsg = "Can't serialize a non-Schema message using SchemaSerializer [" + obj + "]"
      throw new IllegalArgumentException(errorMsg)
    }
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val data = ByteString(bytes).iterator
    val entityName = StringSerializer.fromByteIterator(data)
    val schemaJson = StringSerializer.fromByteIterator(data)
    val schema = new Schema.Parser().parse(schemaJson)
    val duration = data.getLong
    val idleTimeout = if (duration >= 0) {
      FiniteDuration(duration, TimeUnit.MILLISECONDS)
    } else {
      Duration.Undefined
    }
    AddSchema(entityName, schema, idleTimeout)
  }
}
