package chana.avro

import java.io.IOException
import java.io.OutputStream
import java.nio.ByteBuffer
import java.util.BitSet
import org.apache.avro.AvroTypeException
import org.apache.avro.Schema
import org.apache.avro.io.ParsingEncoder
import org.apache.avro.io.parsing.JsonGrammarGenerator
import org.apache.avro.io.parsing.Parser
import org.apache.avro.io.parsing.Symbol
import org.apache.avro.util.Utf8
import org.codehaus.jackson.JsonEncoding
import org.codehaus.jackson.JsonGenerator
import org.codehaus.jackson.util.DefaultPrettyPrinter
import org.codehaus.jackson.util.MinimalPrettyPrinter

/**
 * An {@link Encoder} for Avro's JSON data encoding.
 * </p>
 * Construct using {@link EncoderFactory}.
 * </p>
 * JsonEncoder buffers output, and data may not appear on the output
 * until {@link Encoder#flush()} is called.
 * </p>
 * JsonEncoder is not thread-safe.
 */
object JsonEncoder {
  private val LINE_SEPARATOR = System.getProperty("line.separator")
  private val CHARSET = "ISO-8859-1"

  // by default, one object per line.
  // with pretty option use default pretty printer with root line separator.
  @throws(classOf[IOException])
  private def getJsonGenerator(out: OutputStream, pretty: Boolean): JsonGenerator = {
    if (null == out) {
      throw new NullPointerException("OutputStream cannot be null")
    }
    val g = JSON_FACTORY.createJsonGenerator(out, JsonEncoding.UTF8)
    if (pretty) {
      val pp = new DefaultPrettyPrinter() {
        @throws(classOf[IOException])
        override def writeRootValueSeparator(jg: JsonGenerator) {
          jg.writeRaw(LINE_SEPARATOR)
        }
      }
      g.setPrettyPrinter(pp)
    } else {
      val pp = new MinimalPrettyPrinter()
      pp.setRootValueSeparator(LINE_SEPARATOR)
      g.setPrettyPrinter(pp)
    }
    g
  }

  @throws(classOf[IOException])
  def apply(sc: Schema, out: OutputStream) = new JsonEncoder(sc, getJsonGenerator(out, false))

  @throws(classOf[IOException])
  def apply(sc: Schema, out: OutputStream, pretty: Boolean) = new JsonEncoder(sc, getJsonGenerator(out, pretty))

  @throws(classOf[IOException])
  def apply(sc: Schema, out: JsonGenerator) = new JsonEncoder(sc, out)
}

@throws(classOf[IOException])
final class JsonEncoder(sc: Schema, private var out: JsonGenerator) extends ParsingEncoder with Parser.ActionHandler {
  configure(out)
  private val parser = new Parser(new JsonGrammarGenerator().generate(sc), this)
  /**
   * Has anything been written into the collections?
   */
  protected val isEmpty = new BitSet()

  @throws(classOf[IOException])
  override def flush() {
    parser.processImplicitActions()
    if (out != null) {
      out.flush()
    }
  }

  /**
   * Reconfigures this JsonEncoder to use the output stream provided.
   * <p/>
   * If the OutputStream provided is null, a NullPointerException is thrown.
   * <p/>
   * Otherwise, this JsonEncoder will flush its current output and then
   * reconfigure its output to use a default UTF8 JsonGenerator that writes
   * to the provided OutputStream.
   *
   * @param out
   *          The OutputStream to direct output to. Cannot be null.
   * @throws IOException
   * @return this JsonEncoder
   */
  @throws(classOf[IOException])
  def configure(out: OutputStream): JsonEncoder = {
    configure(JsonEncoder.getJsonGenerator(out, false))
    this
  }

  /**
   * Reconfigures this JsonEncoder to output to the JsonGenerator provided.
   * <p/>
   * If the JsonGenerator provided is null, a NullPointerException is thrown.
   * <p/>
   * Otherwise, this JsonEncoder will flush its current output and then
   * reconfigure its output to use the provided JsonGenerator.
   *
   * @param generator
   *          The JsonGenerator to direct output to. Cannot be null.
   * @throws IOException
   * @return this JsonEncoder
   */
  @throws(classOf[IOException])
  def configure(generator: JsonGenerator): JsonEncoder = {
    if (null == generator) {
      throw new NullPointerException("JsonGenerator cannot be null")
    }
    if (null != parser) {
      flush()
    }
    this.out = generator
    this
  }

  @throws(classOf[IOException])
  def writeNull() {
    parser.advance(Symbol.NULL)
    out.writeNull()
  }

  @throws(classOf[IOException])
  def writeBoolean(b: Boolean) {
    parser.advance(Symbol.BOOLEAN)
    out.writeBoolean(b)
  }

  @throws(classOf[IOException])
  def writeInt(n: Int) {
    parser.advance(Symbol.INT)
    out.writeNumber(n)
  }

  @throws(classOf[IOException])
  def writeLong(n: Long) {
    parser.advance(Symbol.LONG)
    out.writeNumber(n)
  }

  @throws(classOf[IOException])
  def writeFloat(f: Float) {
    parser.advance(Symbol.FLOAT)
    out.writeNumber(f)
  }

  @throws(classOf[IOException])
  def writeDouble(d: Double) {
    parser.advance(Symbol.DOUBLE)
    out.writeNumber(d)
  }

  @throws(classOf[IOException])
  def writeString(utf8: Utf8) {
    writeString(utf8.toString())
  }

  @throws(classOf[IOException])
  override def writeString(str: String) {
    parser.advance(Symbol.STRING)
    if (parser.topSymbol() == Symbol.MAP_KEY_MARKER) {
      parser.advance(Symbol.MAP_KEY_MARKER)
      out.writeFieldName(str)
    } else {
      out.writeString(str)
    }
  }

  @throws(classOf[IOException])
  def writeBytes(bytes: ByteBuffer) {
    if (bytes.hasArray) {
      writeBytes(bytes.array, bytes.position, bytes.remaining)
    } else {
      val b = Array.ofDim[Byte](bytes.remaining)
      bytes.duplicate().get(b)
      writeBytes(b)
    }
  }

  @throws(classOf[IOException])
  def writeBytes(bytes: Array[Byte], start: Int, len: Int) {
    parser.advance(Symbol.BYTES)
    writeByteArray(bytes, start, len)
  }

  @throws(classOf[IOException])
  private def writeByteArray(bytes: Array[Byte], start: Int, len: Int) {
    out.writeString(new String(bytes, start, len, JsonEncoder.CHARSET))
  }

  @throws(classOf[IOException])
  def writeFixed(bytes: Array[Byte], start: Int, len: Int) {
    parser.advance(Symbol.FIXED)
    val top = parser.popSymbol().asInstanceOf[Symbol.IntCheckAction]
    if (len != top.size) {
      throw new AvroTypeException("Incorrect length for fixed binary: expected " + top.size + " but received " + len + " bytes.")
    }
    writeByteArray(bytes, start, len)
  }

  @throws(classOf[IOException])
  def writeEnum(e: Int) {
    parser.advance(Symbol.ENUM)
    val top = parser.popSymbol().asInstanceOf[Symbol.EnumLabelsAction]
    if (e < 0 || e >= top.size) {
      throw new AvroTypeException("Enumeration out of range: max is " + top.size + " but received " + e)
    }
    out.writeString(top.getLabel(e))
  }

  @throws(classOf[IOException])
  def writeArrayStart() {
    parser.advance(Symbol.ARRAY_START)
    out.writeStartArray()
    push()
    isEmpty.set(depth())
  }

  @throws(classOf[IOException])
  def writeArrayEnd() {
    if (!isEmpty.get(pos)) {
      parser.advance(Symbol.ITEM_END)
    }
    pop()
    parser.advance(Symbol.ARRAY_END)
    out.writeEndArray()
  }

  @throws(classOf[IOException])
  def writeMapStart() {
    push()
    isEmpty.set(depth())

    parser.advance(Symbol.MAP_START)
    out.writeStartObject()
  }

  @throws(classOf[IOException])
  def writeMapEnd() {
    if (!isEmpty.get(pos)) {
      parser.advance(Symbol.ITEM_END)
    }
    pop()

    parser.advance(Symbol.MAP_END)
    out.writeEndObject()
  }

  @throws(classOf[IOException])
  override def startItem() {
    if (!isEmpty.get(pos)) {
      parser.advance(Symbol.ITEM_END)
    }
    super.startItem()
    isEmpty.clear(depth())
  }

  @throws(classOf[IOException])
  def writeIndex(unionIndex: Int) {
    parser.advance(Symbol.UNION)
    val top = parser.popSymbol().asInstanceOf[Symbol.Alternative]
    val symbol = top.getSymbol(unionIndex)
    if (symbol != Symbol.NULL) {
      out.writeStartObject()
      out.writeFieldName(top.getLabel(unionIndex))
      parser.pushSymbol(Symbol.UNION_END)
    }
    parser.pushSymbol(symbol)
  }

  @throws(classOf[IOException])
  def doAction(input: Symbol, top: Symbol): Symbol = {
    if (top.isInstanceOf[Symbol.FieldAdjustAction]) {
      val fa = top.asInstanceOf[Symbol.FieldAdjustAction]
      out.writeFieldName(fa.fname)
    } else if (top == Symbol.RECORD_START) {
      out.writeStartObject()
    } else if (top == Symbol.RECORD_END || top == Symbol.UNION_END) {
      out.writeEndObject()
    } else if (top != Symbol.FIELD_END) {
      throw new AvroTypeException("Unknown action symbol " + top)
    }
    null
  }
}

