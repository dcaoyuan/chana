package chana.avro

import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.StringWriter
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import org.codehaus.jackson.JsonNode

/**
 *
 * Encode an Avro value into JSON.
 */
object ToJson {

  /**
   * Serializes a Java Avro value into JSON.
   *
   * When serializing records, fields whose value matches the fields' default value are omitted.
   *
   * @param value the Java value to serialize.
   * @param schema Avro schema of the value to serialize.
   * @return the value encoded as a JSON tree.
   * @throws IOException on error.
   */
  @throws(classOf[IOException])
  def toJsonNode(value: Any, schema: Schema): JsonNode = {
    if (value != null) {
      schema.getType match {
        case Type.NULL    => JSON_NODE_FACTORY.nullNode
        case Type.BOOLEAN => JSON_NODE_FACTORY.booleanNode(value.asInstanceOf[Boolean])
        case Type.DOUBLE  => JSON_NODE_FACTORY.numberNode(value.asInstanceOf[Double])
        case Type.FLOAT   => JSON_NODE_FACTORY.numberNode(value.asInstanceOf[Float])
        case Type.INT     => JSON_NODE_FACTORY.numberNode(value.asInstanceOf[Int])
        case Type.LONG    => JSON_NODE_FACTORY.numberNode(value.asInstanceOf[Long])
        case Type.STRING  => JSON_NODE_FACTORY.textNode(value.asInstanceOf[CharSequence].toString)
        case Type.ENUM =>
          val strVal = value match {
            case x: GenericEnumSymbol => x.toString
            case x: Enum[_]           => x.toString
            case x: String            => x
          }
          JSON_NODE_FACTORY.textNode(strVal) // Enums are represented as strings

        case Type.BYTES | Type.FIXED =>
          // TODO Bytes are represented as strings (BASE64?)...
          throw new RuntimeException("toJsonNode(byte array) not implemented")

        case Type.ARRAY =>
          val jsonArray = JSON_NODE_FACTORY.arrayNode()
          val javaArray = value.asInstanceOf[java.lang.Iterable[_]].iterator

          while (javaArray.hasNext) {
            val element = javaArray.next
            jsonArray.add(toJsonNode(element, schema.getElementType))
          }
          jsonArray

        case Type.MAP =>
          val jsonObject = JSON_NODE_FACTORY.objectNode()
          val javaMap = value.asInstanceOf[java.util.Map[String, Any]].entrySet.iterator

          while (javaMap.hasNext) {
            val entry = javaMap.next
            jsonObject.put(entry.getKey, toJsonNode(entry.getValue, schema.getValueType))
          }
          jsonObject

        case Type.RECORD =>
          val jsonObject = JSON_NODE_FACTORY.objectNode()
          val record = value.asInstanceOf[IndexedRecord]
          //if (record.getSchema != schema) { // TODO not allow multiple version schema?
          //  throw new IOException("Avro schema specifies record type '%s' but got '%s'.".format(schema.getFullName, record.getSchema.getFullName))
          //}
          val fields = schema.getFields.iterator

          while (fields.hasNext) {
            val field = fields.next
            val fieldValue = record.get(field.pos)
            val fieldNode = toJsonNode(fieldValue, field.schema)
            // Outputs the field only if its value differs from the field's default:
            if (field.defaultValue == null || fieldNode != field.defaultValue) {
              jsonObject.put(field.name, fieldNode)
            }
          }
          jsonObject

        case Type.UNION =>
          toUnionJsonNode(value, schema)

        case _ =>
          throw new RuntimeException("Unexpected schema type '%s'.".format(schema))
      }
    } else {
      JSON_NODE_FACTORY.nullNode
    }
  }

  /**
   * Encodes an Avro union into a JSON node.
   *
   * @param value an Avro union to encode.
   * @param schema schema of the union to encode.
   * @return the encoded value as a JSON node.
   * @throws IOException on error.
   */
  @throws(classOf[IOException])
  private def toUnionJsonNode(value: Any, schema: Schema): JsonNode = {
    if (schema.getType != Type.UNION) {
      throw new IOException("Avro schema specifies '%s' but got value: '%s'.".format(schema, value))
    }

    val optionalType = chana.avro.getNonNullOfUnion(schema)
    if (null != optionalType) {
      return if (null == value) JSON_NODE_FACTORY.nullNode else toJsonNode(value, optionalType)
    }

    val typeMap = new java.util.HashMap[Schema.Type, java.util.List[Schema]]()
    val tpes = schema.getTypes.iterator

    while (tpes.hasNext) {
      val tpe = tpes.next
      val typeList = typeMap.get(tpe.getType) match {
        case null =>
          val xs = new java.util.ArrayList[Schema]()
          typeMap.put(tpe.getType, xs)
          xs
        case xs => xs
      }
      typeList.add(tpe)
    }

    //  null is shortened as an immediate JSON null:
    if (null == value) {
      if (!typeMap.containsKey(Type.NULL)) {
        throw new IOException("Avro schema specifies '%s' but got 'null'.".format(schema))
      }
      return JSON_NODE_FACTORY.nullNode
    }

    val union = JSON_NODE_FACTORY.objectNode
    val tpes2 = schema.getTypes.iterator

    while (tpes2.hasNext) {
      val tpe = tpes2.next
      try {
        val actualNode = toJsonNode(value, tpe)
        union.put(tpe.getFullName, actualNode)
        return union
      } catch {
        case ex: IOException => // This type was not the correct union case, ignore...
      }
    }
    throw new IOException("Unable to encode '%s' as union '%s'.".format(value, schema))
  }

  /**
   * Encodes an Avro value into a JSON string.
   *
   * Fields with default values are omitted.
   *
   * @param value Avro value to encode.
   * @param schema Avro schema of the value.
   * @return Pretty string representation of the JSON-encoded value.
   * @throws IOException on error.
   */
  @throws(classOf[IOException])
  def toJsonString(value: Any, schema: Schema): String = {
    val node = toJsonNode(value, schema)
    val stringWriter = new StringWriter()
    val generator = JSON_FACTORY.createJsonGenerator(stringWriter)
    // We have disabled this because we used unions to represent row key formats
    // in the table layout. This is a HACK and needs a better solution.
    // TODO: Find better solution.
    //generator.disable(Feature.QUOTE_FIELD_NAMES);
    JSON_MAPPER.writeValue(generator, node)
    stringWriter.toString
  }

  /**
   * Encodes an Avro record into JSON.
   *
   * @param record Avro record to encode.
   * @return Pretty JSON representation of the record.
   * @throws IOException on error.
   */
  @throws(classOf[IOException])
  def toJsonString(record: IndexedRecord): String = {
    toJsonString(record, record.getSchema)
  }

  /**
   * Standard Avro/JSON encoder.
   *
   * @param value Avro value to encode.
   * @param schema Avro schema of the value.
   * @return JSON-encoded value.
   * @throws IOException on error.
   */
  @throws(classOf[IOException])
  def toAvroJsonString(value: Any, schema: Schema): String = {
    try {
      val jsonOutputStream = new ByteArrayOutputStream()
      val jsonEncoder = EncoderFactory.get.jsonEncoder(schema, jsonOutputStream)
      val writer = new GenericDatumWriter[Any](schema)
      writer.write(value, jsonEncoder)
      jsonEncoder.flush()
      new String(jsonOutputStream.toByteArray)
    } catch {
      case ex: IOException => throw new RuntimeException("Internal error: " + ex)
    }
  }

  /**
   * Standard Avro/JSON encoder.
   *
   * @param record Avro record to encode.
   * @return JSON-encoded value.
   * @throws IOException on error.
   */
  @throws(classOf[IOException])
  def toAvroJsonString(record: IndexedRecord): String = {
    val schema = record.getSchema
    try {
      val jsonOutputStream = new ByteArrayOutputStream()
      val jsonEncoder = EncoderFactory.get().jsonEncoder(schema, jsonOutputStream)
      val writer = new SpecificDatumWriter(record.getClass.asInstanceOf[Class[Any]])
      writer.write(record, jsonEncoder)
      jsonEncoder.flush()
      new String(jsonOutputStream.toByteArray)
    } catch {
      case ex: IOException => throw new RuntimeException("Internal error: " + ex)
    }
  }

}
