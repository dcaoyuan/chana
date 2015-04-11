package wandou.astore.serializer

import akka.actor.ExtendedActorSystem
import akka.serialization.Serializer
import akka.util.ByteString
import java.nio.ByteOrder
import org.apache.avro.Schema

final class SchemaSerializer(system: ExtendedActorSystem) extends Serializer {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  override def identifier: Int = 844372015

  override def includeManifest: Boolean = false

  override def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case schema: Schema =>
      val builder = ByteString.newBuilder
      StringSerializer.appendToByteString(builder, schema.toString)
      builder.result.toArray

    case _ =>
      throw new IllegalArgumentException("Can't serialize a non-Schema message using SchemaSerializer [" + obj + "]")
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val data = ByteString(bytes).iterator
    val schemaJson = StringSerializer.fromByteIterator(data)
    new Schema.Parser().parse(schemaJson)
  }
}
