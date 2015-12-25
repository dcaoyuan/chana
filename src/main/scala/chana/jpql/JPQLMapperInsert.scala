package chana.jpql

import chana.avro
import chana.avro.Insertlog
import chana.avro.UpdateAction
import chana.jpql.nodes._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

final class JPQLMapperInsert(meta: JPQLInsert) extends JPQLEvaluator {

  protected def asToEntity = meta.asToEntity
  protected def asToJoin = meta.asToJoin

  def insertEval(stmt: InsertStatement, record: GenericRecord) = {
    var fieldToValues = List[(Schema.Field, Any)]()

    val entityName = insertClause(stmt.insert, record)
    if (entityName.equalsIgnoreCase(record.getSchema.getName)) {
      var values = valuesClause(stmt.values, record)
      stmt.attributes match {
        case Some(x) =>
          var attrs = attributesClause(x, record)
          while (attrs.nonEmpty) {
            if (values.nonEmpty) {
              val fieldName = attrs.head
              val field = record.getSchema.getField(fieldName)
              fieldToValues ::= (field, values.head)
              attrs = attrs.tail
              values = values.tail
            } else {
              throw JPQLRuntimeException(attrs.head, "does not have corresponding value.")
            }
          }

        case None =>
          val fields = record.getSchema.getFields.iterator
          while (fields.hasNext) {
            val field = fields.next
            if (values.nonEmpty) {
              fieldToValues ::= (field, values.head)
              values = values.tail
            } else {
              throw JPQLRuntimeException(field, "does not have corresponding value.")
            }
          }
      }
    } else {
      // do nothing 
    }

    for ((field, value) <- fieldToValues) yield {
      val prev = record.get(field.pos)
      val rollback = { () => record.put(field.pos, prev) }
      val commit = { () => record.put(field.pos, value) }
      val xpath = "/" + field.name
      val xs = java.util.Arrays.asList(value)
      val bytes = avro.avroEncode(value, Schema.createArray(field.schema)).get
      UpdateAction(commit, rollback, Insertlog(xpath, xs, bytes))
    }
  }

}
