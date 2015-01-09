package wandou.astore.serializer

import akka.actor.ExtendedActorSystem
import akka.serialization.Serializer
import akka.util.{ ByteString }
import java.nio.ByteOrder
import org.apache.avro.generic.GenericRecord
import scala.util.Failure
import wandou.astore.schema.DistributedSchemaBoard
import wandou.avro

class AvroSerializer(val system: ExtendedActorSystem) extends Serializer {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  override def identifier: Int = 844372013

  override def includeManifest: Boolean = true

  override def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case record: GenericRecord => {
      record.getSchema.getFullName
      val r = avro.avroEncode(record, record.getSchema)
      if (r.isSuccess) {
        val builder = ByteString.newBuilder
        StringSerializer.appendToBuilder(builder, record.getSchema.getFullName)
        builder.putBytes(r.get)
        builder.result.toArray
      } else throw r.asInstanceOf[Failure[Array[Byte]]].exception
    }
    case _ => {
      val errorMsg = "Can't serialize a non-Avro message using AvroSerializer [" + obj + "]"
      throw new IllegalArgumentException(errorMsg)
    }
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val data = ByteString(bytes).iterator
    val name = StringSerializer.fromByteIterator(data)

    val schema = DistributedSchemaBoard.schemaOf(name).get
    val payload = Array.ofDim[Byte](data.len)
    data.getBytes(payload)
    val r = avro.avroDecode[GenericRecord](payload, schema)
    if (r.isSuccess) {
      r.get
    } else throw r.asInstanceOf[Failure[Array[Byte]]].exception
  }

}
