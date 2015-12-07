package chana.serializer

import akka.actor.ExtendedActorSystem
import akka.serialization.Serializer
import akka.util.ByteString
import chana.avro
import chana.avro.Binlog
import java.nio.ByteOrder
import org.apache.avro.Schema

final class BinlogSerializer(system: ExtendedActorSystem) extends Serializer {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  override def identifier: Int = 302668158

  override def includeManifest: Boolean = false

  override def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case binlog @ Binlog(xpath, value, schema) =>
      val builder = ByteString.newBuilder

      builder.putByte(binlog.tpe)
      StringSerializer.appendToByteString(builder, xpath)
      StringSerializer.appendToByteString(builder, schema.toString(false))
      val payload = avro.avroEncode(value, schema).get
      builder.putInt(payload.length)
      builder.putBytes(payload)

      builder.result.toArray

    case _ =>
      throw new IllegalArgumentException("Can't serialize a non-Binlog message using BinlogSerializer [" + obj + "]")
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val data = ByteString(bytes).iterator

    val tpe = data.getByte
    val xpath = StringSerializer.fromByteIterator(data)
    val schemaJson = StringSerializer.fromByteIterator(data)
    val schema = new Schema.Parser().parse(schemaJson)
    val len = data.getInt
    val payload = Array.ofDim[Byte](len)
    data.getBytes(payload)
    val value = avro.avroDecode[Any](payload, schema).get

    Binlog(tpe, xpath, value, schema)
  }

}
