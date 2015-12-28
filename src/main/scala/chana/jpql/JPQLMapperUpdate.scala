package chana.jpql

import chana.avro
import chana.avro.Changelog
import chana.avro.RecordFlatView
import chana.avro.UpdateAction
import chana.jpql.nodes._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.codehaus.jackson.JsonNode

final class JPQLMapperUpdate(meta: JPQLUpdate) extends JPQLEvaluator {

  protected def asToEntity = meta.asToEntity
  protected def asToJoin = meta.asToJoin

  def updateEval(stmt: UpdateStatement, record: GenericRecord): List[List[UpdateAction]] = {
    var toUpdates = List[GenericRecord]()
    if (asToJoin.nonEmpty) {
      val joinField = asToJoin.head._2.tail.head
      val recordFlatView = new RecordFlatView(record.asInstanceOf[GenericRecord], joinField)
      val itr = recordFlatView.iterator
      while (itr.hasNext) {
        val rec = itr.next
        val whereCond = stmt.where.fold(true) { x => whereClause(x, rec) }
        if (whereCond) {
          toUpdates ::= rec
        }
      }
    } else {
      val whereCond = stmt.where.fold(true) { x => whereClause(x, record) }
      if (whereCond) {
        toUpdates ::= record
      }
    }

    toUpdates map { toUpdate =>
      stmt.set.assign :: stmt.set.assigns map {
        case SetAssignClause(target, value) =>
          val v = newValue(value, toUpdate)
          target.path match {
            case Left(path) =>
              val qual = qualIdentVar(path.qual, toUpdate)
              val attrPaths = path.attributes map (x => attribute(x, toUpdate))
              val attrPaths1 = normalizeEntityAttrs(qual, attrPaths, toUpdate.getSchema)
              opUpdate(attrPaths1, v, toUpdate)

            case Right(attr) =>
              val fieldName = attribute(attr, toUpdate) // there should be no AS alias
              opUpdate(fieldName, v, toUpdate, "/" + attr)
          }
      }
    }
  }

  private def opUpdate(attr: String, v: Any, record: GenericRecord, xpath: String): UpdateAction = {
    val field = record.getSchema.getField(attr)
    val value = v match {
      case x: JsonNode => avro.FromJson.fromJsonNode(x, field.schema)
      case x           => x
    }

    val prev = record.get(field.pos)
    val rlback = { () => record.put(field.pos, prev) }
    val commit = { () => record.put(field.pos, value) }
    val bytes = avro.avroEncode(value, field.schema).get
    UpdateAction(commit, rlback, Changelog(xpath, value, bytes))
  }

  private def opUpdate(attrPaths: List[String], v: Any, record: GenericRecord): UpdateAction = {
    val xpath = new StringBuilder()
    var paths = attrPaths
    var currTarget: Any = record
    var action: UpdateAction = null
    var schema: Schema = record.getSchema

    while (paths.nonEmpty) {
      val path = paths.head
      paths = paths.tail

      xpath.append("/").append(path)

      if (paths.nonEmpty) {
        currTarget match {
          case fieldRec @ avro.FlattenRecord(underlying, flatField, fieldValue, index) if flatField.name == path =>
            // TODO deep embbed collection fields have the same path(field name), /rec/abc/abc
            flatField.schema.getType match {
              case Schema.Type.ARRAY =>
                xpath.append("[").append(index + 1).append("]")
                currTarget = fieldValue
              case Schema.Type.MAP =>
                val fieldEntry = fieldValue.asInstanceOf[java.util.Map.Entry[CharSequence, _]]
                xpath.append("/@").append(fieldEntry.getKey)
                currTarget = fieldEntry.getValue.asInstanceOf[GenericRecord]
              case _ => throw JPQLRuntimeException(flatField, "is not a collection field: " + path)
            }

          case fieldRec: GenericRecord =>
            currTarget = fieldRec.get(path)

          case arr: java.util.Collection[_]             => throw JPQLRuntimeException(currTarget, "is an avro array when fetch its attribute: " + path) // TODO
          case map: java.util.Map[String, _] @unchecked => throw JPQLRuntimeException(currTarget, "is an avro map when fetch its attribute: " + path) // TODO
          case null                                     => throw JPQLRuntimeException(currTarget, "is null when fetch its attribute: " + paths)
          case _                                        => throw JPQLRuntimeException(currTarget, "is not a record when fetch its attribute: " + paths)
        }

      } else { // reaches the last path, handover to opUpdate(attr: String, v: Any, record: GenericRecord, xpath: String)
        //println("\npath: " + path + ", tar: " + currTarget + ", xpath: " + xpath)
        action = opUpdate(path, v, currTarget.asInstanceOf[GenericRecord], xpath.toString)
      }

    }

    action
  }

  private def getNextSchema(schema: Schema, path: String) = {
    schema.getType match {
      case Schema.Type.ARRAY =>
        val elemSchema = avro.getElementType(schema)
        avro.getNonNull(elemSchema.getField(path).schema)

      case Schema.Type.MAP =>
        val valueSchema = avro.getValueType(schema)
        avro.getNonNull(valueSchema.getField(path).schema)

      case Schema.Type.RECORD | Schema.Type.UNION =>
        avro.getNonNull(schema.getField(path).schema)

      case _ => schema // what happens here? TODO
    }
  }
}
