package chana.jpql

import chana.avro
import chana.avro.Insertlog
import chana.avro.UpdateAction
import chana.jpql
import chana.jpql.nodes._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.IndexedRecord
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.node.ObjectNode

final class JPQLMapperInsert(val id: String, meta: JPQLInsert) extends JPQLEvaluator {

  protected def asToEntity = meta.asToEntity
  protected def asToJoin = meta.asToJoin

  def insertEval(record: IndexedRecord) = {
    val stmt = meta.stmt
    var toInserts = List[IndexedRecord]()
    if (asToJoin.nonEmpty) {
      val joinFieldName = asToJoin.head._2.tail.head
      val joinField = record.getSchema.getField(joinFieldName)
      val recordFlatView = new avro.RecordFlatView(record, joinField)
      val flatRecs = recordFlatView.iterator

      while (flatRecs.hasNext) {
        val rec = flatRecs.next
        val whereCond = stmt.where.fold(true) { x => whereClause(x, rec) }
        if (whereCond) {
          toInserts ::= rec
        }
      }
    } else {
      val whereCond = stmt.where.fold(true) { x => whereClause(x, record) }
      if (whereCond) {
        toInserts ::= record
      }
    }

    var actions = List[UpdateAction]()

    // insert always happens to non flattern record
    val willInsert = toInserts.find {
      case avro.FlattenRecord(underlying, _, _, _) => underlying eq record
      case x                                       => x eq record
    } isDefined

    if (willInsert) {
      val recFields = stmt.attributes match {
        case Some(x) =>
          attributesClause(x, record).map { attr => record.getSchema.getField(attr) }
        case None =>
          import scala.collection.JavaConversions._
          record.getSchema.getFields.toList
      }

      val rows = valuesClause(stmt.values, record)

      for (row <- rows) {
        var fieldToValue = List[(Schema.Field, Any)]()
        var values = row
        var fields = recFields

        while (fields.nonEmpty) {
          if (values.nonEmpty) {
            fieldToValue ::= (fields.head, values.head)
            fields = fields.tail
            values = values.tail
          } else {
            throw JPQLRuntimeException(fields.head, "does not have coresponding value.")
          }
        }

        actions :::= opInsert(fieldToValue, record)
      }
    }

    actions.reverse
  }

  private def opInsert(fieldToValue: List[(Schema.Field, Any)], record: IndexedRecord) = {
    var actions = List[UpdateAction]()

    for ((field, v) <- fieldToValue) yield {
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

    actions
  }

}
