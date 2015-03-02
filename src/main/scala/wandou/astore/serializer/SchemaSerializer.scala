package wandou.astore.serializer

import java.nio.ByteOrder

import akka.actor.ExtendedActorSystem
import akka.serialization.Serializer
import akka.util.ByteString
import org.apache.avro.Schema

class SchemaSerializer(val system: ExtendedActorSystem) extends Serializer {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  override def identifier: Int = 844372015

  override def includeManifest: Boolean = true

  override def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case schema: Schema =>
      val builder = ByteString.newBuilder
      StringSerializer.appendToBuilder(builder, schema.toString)
      builder.result.toArray

    case _ =>
      val errorMsg = "Can't serialize a non-Schema message using SchemaSerializer [" + obj + "]"
      throw new IllegalArgumentException(errorMsg)
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val data = ByteString(bytes).iterator
    val schemaJson = StringSerializer.fromByteIterator(data)

    val schema = new Schema.Parser().parse(schemaJson)
    schema
  }
}
