package chana.avro

import java.io.ByteArrayInputStream
import java.io.IOException
import java.io.InputStream
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificRecord
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.node.ObjectNode

/**
 *
 * Decode a JSON string into an Avro value.
 */
object FromJson {

  /**
   * Decodes a JSON node as an Avro value.
   *
   * Comply with specified default values when decoding records with missing fields.
   *
   * @param json JSON node to decode.
   * @param schema Avro schema of the value to decode.
   * @param to specified or generic value, default generic
   * @return the decoded value.
   * @throws IOException on error.
   */
  @throws(classOf[IOException])
  def fromJson(json: JsonNode, schema: Schema): Any = fromJson(json, schema, false)
  def fromJson(json: JsonNode, schema: Schema, specific: Boolean): Any = {
    schema.getType match {
      case Type.INT =>
        if (json.isInt) {
          json.getIntValue
        } else {
          throw new IOException("Avro schema specifies '%s' but got JSON value: '%s'.".format(schema, json))
        }

      case Type.LONG =>
        if (json.isLong || json.isInt) {
          json.getLongValue
        } else {
          throw new IOException("Avro schema specifies '%s' but got JSON value: '%s'.".format(schema, json))
        }

      case Type.FLOAT =>
        if (json.isDouble || json.isInt || json.isLong) {
          json.getDoubleValue.toFloat
        } else {
          throw new IOException("Avro schema specifies '%s' but got JSON value: '%s'.".format(schema, json))
        }

      case Type.DOUBLE =>
        if (json.isDouble || json.isInt || json.isLong) {
          json.getDoubleValue
        } else {
          throw new IOException("Avro schema specifies '%s' but got JSON value: '%s'.".format(schema, json))
        }

      case Type.STRING =>
        if (json.isTextual) {
          json.getTextValue
        } else {
          throw new IOException("Avro schema specifies '%s' but got JSON value: '%s'.".format(schema, json))
        }

      case Type.BOOLEAN =>
        if (json.isBoolean) {
          json.getBooleanValue
        } else {
          throw new IOException(String.format("Avro schema specifies '%s' but got JSON value: '%s'.", schema, json))
        }

      case Type.ARRAY =>
        if (json.isNull) {
          null
        } else {
          if (json.isArray) {
            val arr = newGenericArray(0, schema)
            val itr = json.getElements

            while (itr.hasNext) {
              val element = itr.next
              addGenericArray(arr, fromJson(element, schema.getElementType, specific))
            }
            arr
          } else {
            throw new IOException("Avro schema specifies '%s' but got JSON value: '%s'.".format(schema, json))
          }
        }

      case Type.MAP =>
        if (json.isNull) {
          null
        } else {
          if (json.isObject) {
            //assert json instanceof ObjectNode; // Help findbugs out.
            val map = new java.util.HashMap[String, Any]()
            val itr = json.asInstanceOf[ObjectNode].getFields

            while (itr.hasNext) {
              val entry = itr.next
              map.put(entry.getKey, fromJson(entry.getValue, schema.getValueType, specific))
            }
            map
          } else {
            throw new IOException("Avro schema specifies '%s' but got JSON value: '%s'.".format(schema, json))
          }
        }

      case Type.RECORD =>
        if (json.isNull) {
          null
        } else {
          if (json.isObject) {
            import scala.collection.JavaConversions._
            var fields = json.getFieldNames.toSet
            val record = if (specific) newSpecificRecord(schema.getFullName) else newGenericRecord(schema)
            val itr = schema.getFields.iterator

            while (itr.hasNext) {
              val field = itr.next
              val name = field.name
              val element = json.get(name)
              if (element != null) {
                val value = fromJson(element, field.schema, specific)
                record.put(field.pos, value)
              } else {
                val defaultValue = if (field.defaultValue != null) {
                  field.defaultValue
                } else {
                  DefaultJsonNode.nodeOf(field)
                }
                record.put(field.pos, fromJson(defaultValue, field.schema, specific))
              }
              fields -= name
            }
            if (!fields.isEmpty) {
              throw new IOException("Error parsing Avro record '%s' with unexpected fields: %s.".format(schema.getFullName, fields.mkString(",")))
            }
            record
          } else {
            throw new IOException("Avro schema specifies '%s' but got JSON value: '%s'.".format(schema, json))
          }
        }

      case Type.UNION =>
        fromUnionJson(json, schema, specific)

      case Type.NULL =>
        if (json.isNull) {
          null
        } else {
          throw new IOException("Avro schema specifies '%s' but got JSON value: '%s'.".format(schema, json))
        }

      case Type.BYTES | Type.FIXED =>
        if (json.isTextual) {
          // TODO: parse string into byte array.
          throw new RuntimeException("Parsing byte arrays is not implemented yet")
        } else {
          throw new IOException("Avro schema specifies '%s' but got non-string JSON value: '%s'.".format(schema, json))
        }

      case Type.ENUM =>
        if (json.isTextual) {
          val enumValStr = json.getTextValue
          if (specific) enumValue(schema.getFullName, enumValStr) else enumGenericValue(schema, enumValStr)
        } else {
          throw new IOException("Avro schema specifies enum '%s' but got non-string JSON value: '%s'.".format(schema, json))
        }

      case _ =>
        throw new RuntimeException("Unexpected schema type: " + schema)
    }
  }

  /**
   * Decodes a union from a JSON node.
   *
   * @param json JSON node to decode.
   * @param schema Avro schema of the union value to decode.
   * @return the decoded value.
   * @throws IOException on error.
   */
  @throws(classOf[IOException])
  private def fromUnionJson(json: JsonNode, schema: Schema): Any = fromUnionJson(json, schema, false)
  private def fromUnionJson(json: JsonNode, schema: Schema, specified: Boolean): Any = {
    if (schema.getType != Type.UNION) {
      throw new IOException("Avro schema specifies '%s' but got JSON value: '%s'.".format(schema, json))
    }

    try {
      val optionalType = getFirstNonNullTypeOfUnion(schema)
      if (optionalType != null) {
        return if (json.isNull) null else fromJson(json, optionalType, specified)
      }
    } catch {
      case ex: IOException => // Union value may be wrapped, ignore.
    }

    /** Map from Avro schema type to list of schemas of this type in the union. */
    val typeToSchemas = new java.util.HashMap[Type, java.util.List[Schema]]()
    val typesIter = schema.getTypes.iterator

    while (typesIter.hasNext) {
      val tpe = typesIter.next
      var types = typeToSchemas.get(tpe.getType)
      if (null == types) {
        types = new java.util.ArrayList[Schema]()
        typeToSchemas.put(tpe.getType, types)
      }
      types.add(tpe)
    }

    if (json.isObject && (json.size == 1)) {
      val entry = json.getFields.next()
      val typeName = entry.getKey
      val actualNode = entry.getValue

      val typesIter = schema.getTypes.iterator

      while (typesIter.hasNext) {
        val tpe = typesIter.next
        if (tpe.getFullName == typeName) {
          return fromJson(actualNode, tpe, specified)
        }
      }
    }

    val typesIter1 = schema.getTypes.iterator

    while (typesIter1.hasNext) {
      val tpe = typesIter1.next
      try {
        return fromJson(json, tpe, specified)
      } catch {
        case ex: IOException => // Wrong union type case.
      }
    }

    throw new IOException("Unable to decode JSON '%s' for union '%s'.".format(json, schema))
  }

  /**
   * Decodes a JSON encoded record.
   *
   * @param json JSON tree to decode, encoded as a string.
   * @param schema Avro schema of the value to decode.
   * @return the decoded value.
   * @throws IOException on error.
   */
  @throws(classOf[IOException])
  def fromJson(json: String, schema: Schema): Any = fromJson(json, schema, false)
  def fromJson(json: String, schema: Schema, specific: Boolean): Any = {
    val parser = JSON_FACTORY.createJsonParser(json)
    val root = JSON_MAPPER.readTree(parser)
    parser.close()
    fromJson(root, schema, specific)
  }

  /**
   * Decodes a JSON encoded record.
   *
   * @param json JSON tree to decode, encoded as a string.
   * @param schema Avro schema of the value to decode.
   * @return the decoded value.
   * @throws IOException on error.
   */
  @throws(classOf[IOException])
  def fromJson(json: Array[Byte], schema: Schema): Any = fromJson(json, schema, false)
  def fromJson(json: Array[Byte], schema: Schema, specific: Boolean): Any = {
    val parser = JSON_FACTORY.createJsonParser(json)
    val root = JSON_MAPPER.readTree(parser)
    parser.close()
    fromJson(root, schema, specific)
  }

  @throws(classOf[IOException])
  def fromJson(json: InputStream, schema: Schema): Any = fromJson(json, schema, false)
  def fromJson(json: InputStream, schema: Schema, specific: Boolean): Any = {
    val parser = JSON_FACTORY.createJsonParser(json)
    val root = JSON_MAPPER.readTree(parser)
    parser.close()
    fromJson(root, schema, specific)
  }

  /**
   * Instantiates a specific record by name.
   *
   * @param fullName Fully qualified record name to instantiate.
   * @return a brand-new specific record instance of the given class.
   * @throws IOException on error.
   */
  @throws(classOf[IOException])
  private def newGenericRecord(schema: Schema) = new GenericData.Record(schema)

  /**
   * Instantiates a specific record by name.
   *
   * @param fullName Fully qualified record name to instantiate.
   * @return a brand-new specific record instance of the given class.
   * @throws IOException on error.
   */
  @throws(classOf[IOException])
  private def newSpecificRecord(fullName: String): SpecificRecord = {
    try {
      val klass = Class.forName(fullName)
      klass.newInstance().asInstanceOf[SpecificRecord]
    } catch {
      case ex: ClassNotFoundException => throw new IOException("Error while deserializing JSON: '%s' class not found.".format(fullName))
      case ex: IllegalAccessException => throw new IOException("Error while deserializing JSON: cannot access '%s'.".format(fullName))
      case ex: InstantiationException => throw new IOException("Error while deserializing JSON: cannot instantiate '%s'.".format(fullName))
    }
  }

  /**
   * Looks up an Avro enum by name and string value.
   *
   * @param fullName Fully qualified enum name to look-up.
   * @param value Enum value as a string.
   * @return the Java enum value.
   * @throws IOException on error.
   */
  @throws(classOf[IOException])
  private def enumGenericValue(schema: Schema, value: String): Any = {
    new GenericData.EnumSymbol(schema, value)
  }

  /**
   * Looks up an Avro enum by name and string value.
   *
   * @param fullName Fully qualified enum name to look-up.
   * @param value Enum value as a string.
   * @return the Java enum value.
   * @throws IOException on error.
   */
  @throws(classOf[IOException])
  private def enumValue(fullName: String, value: String): Any = {
    try {
      Class.forName(fullName) match {
        case enumClass if classOf[Enum[_]].isAssignableFrom(enumClass) => enumValue(enumClass, value)
        case _ => throw new IOException("Error while deserializing JSON: '%s' enum class not found.".format(fullName))
      }
    } catch {
      case ex: ClassNotFoundException => throw new IOException("Error while deserializing JSON: '%s' enum class not found.".format(fullName))
    }
  }

  /**
   * We need the compiler to believe that the Class[_] we have is actually a Class[T <: Enum[T]]
   * (so of course, a preliminary test that this is indeed a Java enum — as done
   * in your code — is needed). So we cast cls to Class[T], where T was inferred
   * by the compiler to be <: Enum[T]. But the compiler still has to find a suitable T,
   * and defaults to Nothing here. So, as far as the compiler is concerned, cls.asInstanceOf[Class[T]]
   * is a Class[Nothing]. This is temporarily OK since it can be used to call Enum.valueOf —
   * the problem is that the inferred return type of valueOf is then, naturally,
   * Nothing as well. And here we have a problem, because the compiler will insert
   * an exception when we try to actually use an instance of type Nothing. So, we
   * finally cast the return value of valueOf to an Enum[_].
   *
   * The trick is then to always let the compiler infer the type argument to enumValueOf
   * and never try to specify it ourselves (since we're not supposed to know it anyway) —
   * and thus to extract the call to Enum.valueOf in another method, giving the
   * compiler a chance to bind a T <: Enum[T].
   */
  private def enumValue[T <: Enum[T]](cls: Class[_], stringValue: String): Enum[_] =
    Enum.valueOf(cls.asInstanceOf[Class[T]], stringValue).asInstanceOf[Enum[_]]

  /**
   * Standard Avro JSON decoder.
   *
   * @param json JSON string to decode.
   * @param schema Schema of the value to decode.
   * @return the decoded value.
   * @throws IOException on error.
   */
  @throws(classOf[IOException])
  def fromAvroJsonString(json: String, schema: Schema, specific: Boolean = false): Any = {
    val jsonInput = new ByteArrayInputStream(json.getBytes("UTF-8"))
    val decoder = DecoderFactory.get.jsonDecoder(schema, jsonInput)
    val reader = if (specific) new SpecificDatumReader[Any](schema) else new GenericDatumReader[Any](schema)
    reader.read(null.asInstanceOf[Any], decoder)
  }

  def getFirstNonNullTypeOfUnion(schema: Schema) = {
    val tpes = schema.getTypes.iterator
    var firstNonNullType: Schema = null

    while (tpes.hasNext && firstNonNullType == null) {
      val tpe = tpes.next
      if (tpe.getType != Type.NULL) {
        firstNonNullType = tpe
      }
    }
    if (firstNonNullType != null) firstNonNullType else schema.getTypes.get(0)
  }
}
