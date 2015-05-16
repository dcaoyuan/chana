package chana.serializer

import akka.actor.ExtendedActorSystem
import akka.serialization.Serializer
import akka.util.ByteString
import chana.avro
import java.nio.ByteOrder
import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericContainer }
import scala.util.Failure
import scala.util.Success

final class AvroSerializer(system: ExtendedActorSystem) extends Serializer {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  override def identifier: Int = 844372013

  override def includeManifest: Boolean = false

  override def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case container: GenericContainer =>
      avro.avroEncode(container, container.getSchema) match {
        case Success(x) =>
          val builder = ByteString.newBuilder
          // FIXME: how to avoid serializing the schema json string
          StringSerializer.appendToByteString(builder, container.getSchema.toString)
          builder.putBytes(x)
          builder.result.toArray
        case Failure(ex) => throw ex
      }

    case _ =>
      throw new IllegalArgumentException("Can't serialize a non-Avro message using AvroSerializer [" + obj + "]")
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val data = ByteString(bytes).iterator
    val schemaJson = StringSerializer.fromByteIterator(data)

    val schema = new Schema.Parser().parse(schemaJson)
    val payload = Array.ofDim[Byte](data.len)
    data.getBytes(payload)
    avro.avroDecode[GenericContainer](payload, schema) match {
      case Success(x)  => x
      case Failure(ex) => throw ex
    }
  }

}
