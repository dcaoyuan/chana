package chana.serializer

import akka.util.ByteString
import chana.avro
import chana.UpdatedFields
import java.nio.ByteOrder
import org.apache.avro.Schema

import scala.collection.JavaConversions._
import scala.util.{ Failure, Success }

final class AvroMarshaler(val schema: Schema) {

  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  val fieldPosToSchemaMap = schema.getFields.map { field =>
    field.pos() -> field.schema()
  }.toMap

  def marshal(avroData: Any) = {
    AvroMarshaler.marshal(avroData, schema)
  }

  def unmarshal(avroBytes: Array[Byte]) = {
    AvroMarshaler.unmarshal(avroBytes, schema)
  }

  def marshalField(pos: Int, value: Any) = {
    val builder = ByteString.newBuilder
    builder.putInt(pos)
    val fieldSchema = fieldPosToSchemaMap(pos)
    val valueMarshaled = AvroMarshaler.marshal(value, fieldSchema)
    builder.putInt(valueMarshaled.length)
    builder.putBytes(valueMarshaled)
    builder.result().toArray
  }

  def marshalFields(fields: List[(Int, Any)]): Array[Byte] = {
    val builder = ByteString.newBuilder
    builder.putInt(fields.size)
    builder.putBytes(
      fields.flatMap { field =>
        marshalField(field._1, field._2)
      }.toArray)
    builder.result().toArray
  }

  def marshalFields(fields: UpdatedFields): Array[Byte] = {
    marshalFields(fields.updatedFields)
  }

  def unmarshalField(avroBytes: Array[Byte]) = {
    val bytes = ByteString(avroBytes).iterator
    val pos = bytes.getInt
    val length = bytes.getInt
    val payload = Array.ofDim[Byte](length)
    bytes.getBytes(payload)
    val value = AvroMarshaler.unmarshal(payload, fieldPosToSchemaMap(pos))
    (pos, value)
  }

  def unmarshalFields(avroBytes: Array[Byte]): List[(Int, Any)] = {
    var res = List[(Int, Any)]()
    val bytes = ByteString(avroBytes).iterator
    val size = bytes.getInt
    var i = 0
    while (i < size) {
      val pos = bytes.getInt
      val length = bytes.getInt
      val payload = Array.ofDim[Byte](length)
      bytes.getBytes(payload)
      val value = AvroMarshaler.unmarshal(payload, fieldPosToSchemaMap(pos))
      res ::= (pos, value)
      i += 1
    }
    res.reverse
  }
}

object AvroMarshaler {
  def marshal(avroData: Any, schema: Schema) = {
    avro.avroEncode(avroData, schema) match {
      case Success(x)  => x
      case Failure(ex) => throw ex
    }
  }

  def unmarshal(avroBytes: Array[Byte], schema: Schema) = {
    avro.avroDecode[Any](avroBytes, schema) match {
      case Success(x)  => x
      case Failure(ex) => throw ex
    }
  }
}
