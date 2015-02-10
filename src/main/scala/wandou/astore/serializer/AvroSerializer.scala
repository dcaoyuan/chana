package wandou.astore.serializer

import java.nio.ByteOrder

import akka.actor.ExtendedActorSystem
import akka.serialization.Serializer
import akka.util.ByteString
import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericContainer, GenericRecord }
import wandou.avro

import scala.util.Failure

class AvroSerializer(val system: ExtendedActorSystem) extends Serializer {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  override def identifier: Int = 844372013

  override def includeManifest: Boolean = true

  override def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case record: GenericContainer =>
      val r = avro.avroEncode(record, record.getSchema)
      if (r.isSuccess) {
        val builder = ByteString.newBuilder
        // FIXME: how to avoid serializing the schema json string
        StringSerializer.appendToBuilder(builder, record.getSchema.toString)
        builder.putBytes(r.get)
        builder.result.toArray
      } else throw r.asInstanceOf[Failure[Array[Byte]]].exception

    case _ => {
      val errorMsg = "Can't serialize a non-Avro message using AvroSerializer [" + obj + "]"
      throw new IllegalArgumentException(errorMsg)
    }
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val data = ByteString(bytes).iterator
    val schemaJson = StringSerializer.fromByteIterator(data)

    val schema = new Schema.Parser().parse(schemaJson)
    val payload = Array.ofDim[Byte](data.len)
    data.getBytes(payload)
    val r = avro.avroDecode[GenericRecord](payload, schema)
    if (r.isSuccess) {
      r.get
    } else throw r.asInstanceOf[Failure[Array[Byte]]].exception
  }

}
