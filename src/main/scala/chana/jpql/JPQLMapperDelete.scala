package chana.jpql

import chana.avro
import chana.avro.Deletelog
import chana.avro.UpdateAction
import chana.jpql.nodes._
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

final class JPQLMapperDelete(meta: JPQLDelete) extends JPQLEvaluator {

  protected def asToEntity = meta.asToEntity
  protected def asToJoin = meta.asToJoin

  def deleteEval(stmt: DeleteStatement, record: IndexedRecord): List[UpdateAction] = {
    var toDeletes = List[IndexedRecord]()
    if (asToJoin.nonEmpty) {
      val joinFieldName = asToJoin.head._2.tail.head
      val joinField = record.getSchema.getField(joinFieldName)
      val recordFlatView = new avro.RecordFlatView(record, joinField)
      val flatRecs = recordFlatView.iterator

      while (flatRecs.hasNext) {
        val rec = flatRecs.next
        val whereCond = stmt.where.fold(true) { x => whereClause(x, rec) }
        if (whereCond) {
          toDeletes ::= rec
        }
      }
    } else {
      val whereCond = stmt.where.fold(true) { x => whereClause(x, record) }
      if (whereCond) {
        toDeletes ::= record
      }
    }

    var actions = List[UpdateAction]()

    // If attributes is set (should be collection fields), we'll clear these collection fields 
    stmt.attributes match {
      case Some(x) =>
        val willDelete = toDeletes.find {
          case avro.FlattenRecord(underlying, _, _, _) => underlying eq record
          case x                                       => x eq record
        } isDefined

        if (willDelete) {
          val fields = attributesClause(x, record).map { attr => record.getSchema.getField(attr) }.iterator

          while (fields.hasNext) {
            val field = fields.next
            field.schema.getType match {
              case Schema.Type.ARRAY =>
                record.get(field.pos) match {
                  case null => // Do nothing
                  case arr: java.util.Collection[_] =>
                    val commit = { () => arr.clear }
                    val xpath = "/" + field.name
                    actions ::= UpdateAction(commit, { () => () }, avro.Clearlog(xpath))
                }

              case Schema.Type.MAP =>
                record.get(field.pos) match {
                  case null => // Do nothing
                  case map: java.util.Map[String, _] @unchecked =>
                    val commit = { () => map.clear }
                    val xpath = "/" + field.name
                    actions ::= UpdateAction(commit, { () => () }, avro.Clearlog(xpath))
                }

              case _ => throw JPQLRuntimeException(field, "is not a collection field: ")
            }
          }
        }

      case None =>

        toDeletes map {
          case fieldRec @ avro.FlattenRecord(underlying, flatField, fieldValue, index) =>

            flatField.schema.getType match {
              case Schema.Type.ARRAY =>
                val arr = underlying.get(flatField.pos).asInstanceOf[java.util.Collection[Any]]

                val xpath = "/" + flatField.name
                val keys = java.util.Arrays.asList(index + 1)

                val rlback = { () => () }
                val commit = { () => avro.arrayRemove(arr, List(index)) }
                actions ::= UpdateAction(commit, rlback, Deletelog(xpath, keys))

              case Schema.Type.MAP =>
                val map = underlying.get(flatField.pos).asInstanceOf[java.util.Map[String, Any]]

                val xpath = "/" + flatField.name
                val fieldEntry = fieldValue.asInstanceOf[java.util.Map.Entry[CharSequence, Any]]
                val k = fieldEntry.getKey.toString
                val keys = java.util.Arrays.asList(k)
                val prev = fieldEntry.getValue

                val rlback = { () => map.put(k, prev) }
                val commit = { () => map.remove(k) }
                actions ::= UpdateAction(commit, rlback, Deletelog(xpath, keys))

              case _ => throw JPQLRuntimeException(flatField, "is not a collection field: ")
            }

          case x =>
            actions ::= UpdateAction(null, null, Deletelog("/", java.util.Collections.EMPTY_LIST))
        }
    }

    actions.reverse
  }
}
