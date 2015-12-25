package chana.jpql

import chana.avro
import chana.avro.Changelog
import chana.avro.RecordFlatView
import chana.avro.UpdateAction
import chana.jpql.nodes._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

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
              updateValue(attrPaths1, v, toUpdate)

            case Right(attr) =>
              val fieldName = attribute(attr, toUpdate) // there should be no AS alias
              updateValue(fieldName, v, toUpdate)
          }
      }
    }
  }

  private def updateValue(attr: String, v: Any, record: GenericRecord): UpdateAction = {
    val field = record.getSchema.getField(attr)
    val prev = record.get(field.pos)
    val rlback = { () => record.put(field.pos, prev) }
    val commit = { () => record.put(field.pos, v) }
    val xpath = "/" + field.name
    val bytes = avro.avroEncode(v, field.schema).get
    UpdateAction(commit, rlback, Changelog(xpath, v, bytes))
  }

  private def updateValue(attrPaths: List[String], v: Any, record: GenericRecord): UpdateAction = {
    var paths = attrPaths
    var currTarget: Any = record
    var action: UpdateAction = null
    var schema: Schema = record.getSchema

    while (paths.nonEmpty) {
      val path = paths.head
      paths = paths.tail

      schema = avro.getNonNull(schema.getField(path).schema)

      currTarget match {
        case fieldRec: GenericRecord =>
          if (paths.isEmpty) { // reaches last path
            val field = fieldRec.getSchema.getField(path)
            val prev = fieldRec.get(field.pos)
            val rlback = { () => fieldRec.put(field.pos, prev) }
            val commit = { () => fieldRec.put(field.pos, v) }
            val xpath = attrPaths.mkString("/", "/", "") // TODO
            val bytes = avro.avroEncode(v, field.schema).get
            action = UpdateAction(commit, rlback, Changelog(xpath, v, bytes))
          } else {
            currTarget = fieldRec.get(path)
          }

        case arr: java.util.Collection[_]             => throw JPQLRuntimeException(currTarget, "is an avro array when fetch its attribute: " + path) // TODO
        case map: java.util.Map[String, _] @unchecked => throw JPQLRuntimeException(currTarget, "is an avro map when fetch its attribute: " + path) // TODO
        case null                                     => throw JPQLRuntimeException(currTarget, "is null when fetch its attribute: " + paths)
        case _                                        => throw JPQLRuntimeException(currTarget, "is not a record when fetch its attribute: " + paths)
      }
    }

    action
  }
}
