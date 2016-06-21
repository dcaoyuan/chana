package chana.avro

import java.io.ByteArrayOutputStream
import java.io.IOException
import org.apache.avro.AvroRuntimeException
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.data.RecordBuilderBase
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.apache.avro.io.parsing.ResolvingGrammarGenerator

object DefaultRecordBuilder {
  /**
   * Creates a RecordBuilder for building Record instances.
   * @param schema the schema associated with the record class.
   */
  def apply(schema: Schema) = {
    new DefaultRecordBuilder(new GenericData.Record(schema), GenericData.get)
  }

  /**
   * Creates a RecordBuilder by copying an existing RecordBuilder.
   * @param other the GenericRecordBuilder to copy.
   */
  def apply(other: DefaultRecordBuilder) = {
    new DefaultRecordBuilder(new GenericData.Record(other.record, /* deepCopy = */ true), GenericData.get)
  }

  /**
   * Creates a RecordBuilder by copying an existing record instance.
   * @param other the record instance to copy.
   */
  def apply(other: Record) = {
    val record = new GenericData.Record(other, /* deepCopy = */ true)
    val data = GenericData.get

    val builder = new DefaultRecordBuilder(record, data)
    // Set all fields in the RecordBuilder that are set in the record
    val fields = record.getSchema.getFields.iterator

    while (fields.hasNext) {
      val f = fields.next
      val value = other.get(f.pos)
      // Only set the value if it is not null, if the schema type is null, 
      // or if the schema type is a union that accepts nulls.
      if (RecordBuilderBase.isValidValue(f, value)) {
        builder.set(f, data.deepCopy(f.schema, value))
      }
    }
    builder
  }
}

/**
 * A RecordBuilder for generic records, fills in default values
 * for fields if they are not specified.
 */
class DefaultRecordBuilder private (private[DefaultRecordBuilder] val record: GenericData.Record, data: GenericData)
    extends RecordBuilderBase[Record](record.getSchema, GenericData.get) {

  /**
   * Gets the value of a field.
   * @param fieldName the name of the field to get.
   * @return the value of the field with the given name, or null if not set.
   */
  def get(fieldName: String): AnyRef = get(schema.getField(fieldName))

  /**
   * Gets the value of a field.
   * @param field the field to get.
   * @return the value of the given field, or null if not set.
   */
  def get(field: Field): AnyRef = get(field.pos)

  /**
   * Gets the value of a field.
   * @param pos the position of the field to get.
   * @return the value of the field with the given position, or null if not set.
   */
  protected def get(pos: Int): AnyRef = record.get(pos)

  /**
   * Sets the value of a field.
   * @param fieldName the name of the field to set.
   * @param value the value to set.
   * @return a reference to the RecordBuilder.
   */
  def set(fieldName: String, value: AnyRef): DefaultRecordBuilder = set(schema.getField(fieldName), value)

  /**
   * Sets the value of a field.
   * @param field the field to set.
   * @param value the value to set.
   * @return a reference to the RecordBuilder.
   */
  def set(field: Field, value: AnyRef): DefaultRecordBuilder = set(field, field.pos, value)

  /**
   * Sets the value of a field.
   * @param pos the field to set.
   * @param value the value to set.
   * @return a reference to the RecordBuilder.
   */
  protected def set(pos: Int, value: AnyRef): DefaultRecordBuilder = set(fields()(pos), pos, value)

  /**
   * Sets the value of a field.
   * @param field the field to set.
   * @param pos the position of the field.
   * @param value the value to set.
   * @return a reference to the RecordBuilder.
   */
  private def set(field: Field, pos: Int, value: AnyRef): DefaultRecordBuilder = {
    validate(field, value)
    record.put(pos, value)
    fieldSetFlags()(pos) = true
    this
  }

  /**
   * Checks whether a field has been set.
   * @param fieldName the name of the field to check.
   * @return true if the given field is non-null; false otherwise.
   */
  def has(fieldName: String): Boolean = has(schema().getField(fieldName))

  /**
   * Checks whether a field has been set.
   * @param field the field to check.
   * @return true if the given field is non-null; false otherwise.
   */
  def has(field: Field): Boolean = has(field.pos)

  /**
   * Checks whether a field has been set.
   * @param pos the position of the field to check.
   * @return true if the given field is non-null; false otherwise.
   */
  protected def has(pos: Int): Boolean = fieldSetFlags()(pos)

  /**
   * Clears the value of the given field.
   * @param fieldName the name of the field to clear.
   * @return a reference to the RecordBuilder.
   */
  def clear(fieldName: String): DefaultRecordBuilder = clear(schema.getField(fieldName))

  /**
   * Clears the value of the given field.
   * @param field the field to clear.
   * @return a reference to the RecordBuilder.
   */
  def clear(field: Field): DefaultRecordBuilder = clear(field.pos)

  /**
   * Clears the value of the given field.
   * @param pos the position of the field to clear.
   * @return a reference to the RecordBuilder.
   */
  protected def clear(pos: Int): DefaultRecordBuilder = {
    record.put(pos, null)
    fieldSetFlags()(pos) = false
    this
  }

  def build(): Record = {
    val record = try {
      new GenericData.Record(schema)
    } catch {
      case ex: Exception => throw new AvroRuntimeException(ex)
    }

    var i = 0
    val fs = fields()
    while (i < fs.length) {
      val field = fs(i)
      val value = try {
        getWithDefault(field)
      } catch {
        case ex: IOException => throw new AvroRuntimeException(ex)
      }
      if (value != null) {
        record.put(field.pos, value)
      }
      i += 1
    }

    record
  }

  /**
   * Gets the value of the given field.
   * If the field has been set, the set value is returned (even if it's null).
   * If the field hasn't been set and has a default value, the default value
   * is returned.
   * @param field the field whose value should be retrieved.
   * @return the value set for the given field, the field's default value,
   * or null.
   * @throws IOException
   */
  @throws(classOf[IOException])
  private def getWithDefault(field: Field): AnyRef = {
    if (fieldSetFlags()(field.pos))
      record.get(field.pos)
    else
      defaultValue(field)
  }

  /**
   * Gets the default value of the given field, if any.
   * @param field the field whose default value should be retrieved.
   * @return the default value associated with the given field,
   * or null if none is specified in the schema.
   * @throws IOException
   */
  @throws(classOf[IOException])
  override protected def defaultValue(field: Field): AnyRef = {
    data.deepCopy(field.schema, getDefaultValue(field))
  }

  private val defaultValueCache = java.util.Collections.synchronizedMap(new java.util.WeakHashMap[Field, AnyRef]())
  /**
   * Gets the default value of the given field, if any was specified, otherwise use DefaultJsonNode's value.
   * @param field the field whose default value should be retrieved.
   * @return the default value associated with the given field,
   * or null if none is specified in the schema.
   */
  private def getDefaultValue(field: Field): AnyRef = {
    var json = field.defaultValue
    if (json == null)
      //throw new AvroRuntimeException("Field " + field + " not set and has no default value")
      json = DefaultJsonNode.of(field)
    if (json.isNull &&
      (field.schema.getType == Schema.Type.NULL ||
        (field.schema.getType == Schema.Type.UNION &&
          field.schema.getTypes.get(0).getType == Schema.Type.NULL))) {
      return null
    }

    // Check the cache
    var defaultValue = defaultValueCache.get(field)

    // If not cached, get the default Java value by encoding the default JSON
    // value and then decoding it:
    if (defaultValue == null)
      try {
        val baos = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get.binaryEncoder(baos, null)
        ResolvingGrammarGenerator.encode(encoder, field.schema, json)
        encoder.flush()
        val decoder = DecoderFactory.get.binaryDecoder(baos.toByteArray, null)
        defaultValue = createDatumReader[AnyRef](field.schema, data).read(null, decoder)

        defaultValueCache.put(field, defaultValue)
      } catch {
        case ex: IOException => throw new AvroRuntimeException(ex)
      }

    defaultValue
  }

  /** Returns a {@link DatumReader} for this kind of data. */
  private def createDatumReader[T](schema: Schema, data: GenericData): DatumReader[T] = {
    new GenericDatumReader[T](schema, schema, data)
  }

  override def hashCode: Int = {
    val prime = 31
    var result = super.hashCode()
    result = prime * result + (if (record == null) 0 else record.hashCode)
    result
  }

  override def equals(obj: Any): Boolean = {
    if (this == obj)
      return true
    if (!super.equals(obj))
      return false
    if (getClass != obj.getClass)
      return false
    val other = obj.asInstanceOf[DefaultRecordBuilder]
    if (record == null) {
      if (other.record != null)
        return false
    } else if (!record.equals(other.record))
      return false
    true
  }
}
