package chana.jpql

import chana.avro
import chana.avro.Insertlog
import chana.avro.UpdateAction
import chana.jpql
import chana.jpql.nodes._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.node.ObjectNode

final class JPQLMapperInsert(meta: JPQLInsert) extends JPQLEvaluator {

  protected def asToEntity = meta.asToEntity
  protected def asToJoin = meta.asToJoin

  def insertEval(stmt: InsertStatement, record: GenericRecord) = {
    var fieldToValues = List[(Schema.Field, Any)]()

    val entityName = insertClause(stmt.insert, record)
    if (entityName.equalsIgnoreCase(record.getSchema.getName)) {
      val rows = valuesClause(stmt.values, record)
      import scala.collection.JavaConversions._
      val fields = stmt.attributes match {
        case Some(x) =>
          attributesClause(x, record).map { attr => record.getSchema.getField(attr) }.iterator
        case None =>
          record.getSchema.getFields.iterator.toIterator
      }

      for (row <- rows) {
        var values = row

        while (fields.hasNext) {
          val field = fields.next
          if (values.nonEmpty) {
            fieldToValues ::= (field, values.head)
            values = values.tail
          } else {
            throw JPQLRuntimeException(field, "does not have coresponding value.")
          }
        }
      }
    } else {
      // do nothing 
    }

    opInsert(fieldToValues, record)
  }

  private def opInsert(fieldToValues: List[(Schema.Field, Any)], record: GenericRecord) = {
    var actions = List[UpdateAction]()

    for ((field, v) <- fieldToValues) yield {
      field.schema.getType match {
        case Schema.Type.ARRAY =>
          val elemSchema = avro.getElementType(field.schema)
          val value = v match {
            case x: JsonNode => avro.FromJson.fromJsonNode(x, elemSchema)
            case x           => x
          }

          val arr = record.get(field.pos) match {
            case null                                     => new GenericData.Array[Any](0, field.schema)
            case xs: java.util.Collection[Any] @unchecked => xs
          }

          val prev = GenericData.get().deepCopy(field.schema, record.get(field.pos))
          val rlback = { () => record.put(field.pos, prev) }
          val commit = { () => arr.add(value) }
          val xpath = "/" + field.name
          val xs = java.util.Arrays.asList(value)
          val bytes = avro.avroEncode(xs, field.schema).get
          actions ::= UpdateAction(commit, rlback, Insertlog(xpath, xs, bytes))

        case Schema.Type.MAP =>
          val valueSchema = avro.getValueType(field.schema)
          val (key, value) = v match {
            case x: ObjectNode =>
              val kvs = x.getFields
              // should contain only one entry
              if (kvs.hasNext) {
                val kv = kvs.next
                val k = kv.getKey
                (k, avro.FromJson.fromJsonNode(kv.getValue, valueSchema))
              } else {
                throw JPQLRuntimeException(x, "does not contain anything")
              }
            case (k: String, v) => (k, v)
            case _              => throw JPQLRuntimeException(v, "does not contain anything")
          }

          val map = record.get(field.pos) match {
            case null                                      => new java.util.HashMap[String, Any]()
            case xs: java.util.Map[String, Any] @unchecked => xs
          }

          val prev = GenericData.get().deepCopy(field.schema, record.get(field.pos))
          val rlback = { () => record.put(field.pos, prev) }
          val commit = { () => map.put(key, value) }
          val xpath = "/" + field.name
          val xs = new java.util.LinkedHashMap[String, Any]()
          xs.put(key, value)
          val bytes = avro.avroEncode(xs, field.schema).get
          actions ::= UpdateAction(commit, rlback, Insertlog(xpath, xs, bytes))

        case _ =>
          val value = v match {
            case x: JsonNode => avro.FromJson.fromJsonNode(x, field.schema)
            case x           => x
          }

          val prev = GenericData.get().deepCopy(field.schema, record.get(field.pos))
          val rlback = { () => record.put(field.pos, prev) }
          val commit = { () => record.put(field.pos, value) }
          val xpath = "/" + field.name
          val xs = java.util.Arrays.asList(value)
          val bytes = avro.avroEncode(value, Schema.createArray(field.schema)).get
          actions ::= UpdateAction(commit, rlback, Insertlog(xpath, xs, bytes))
      }
    }

    actions.reverse
  }

}
