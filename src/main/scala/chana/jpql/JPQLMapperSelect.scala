package chana.jpql

import chana.avro
import chana.avro.RecordFlatView
import chana.jpql.nodes._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.IndexedRecord

final case class DeletedRecord(id: String)

/**
 * AvroProjection should extends Serializable to got BinaryProjection/RemoveProjection
 * using custom serializer
 */
sealed trait AvroProjection extends Serializable { def id: String }
final case class BinaryProjection(id: String, projection: Array[Byte]) extends AvroProjection
final case class RemoveProjection(id: String) extends AvroProjection // used to remove

final class JPQLMapperSelect(val id: String, meta: JPQLSelect) extends JPQLEvaluator {
  private val projection = new Record(meta.projectionSchema.head) // TODO multiple projections

  protected def asToEntity = meta.asToEntity
  protected def asToJoin = meta.asToJoin

  override protected def forceGather = true

  /**
   * Main Entrance for select statement during mapping.
   */
  def gatherProjection(record: IndexedRecord): AvroProjection = {
    meta.stmt match {
      case SelectStatement(select, from, where, groupby, having, orderby) =>

        if (asToJoin.nonEmpty) {
          val joinFieldName = asToJoin.head._2.tail.head
          val joinField = record.getSchema.getField(joinFieldName)
          val recordFlatView = new RecordFlatView(record, joinField)
          val flatRecs = recordFlatView.iterator
          var hasResult = false
          while (flatRecs.hasNext) {
            val rec = flatRecs.next
            // aggregate function can not be applied in WhereClause, so we can decide here

            // we should evaluate selectClause first to get all alias ready.
            selectClause(select, rec, toGather = false)
            val whereCond = where.fold(true) { x => whereClause(x, rec) }
            if (whereCond) {
              selectClause(select, rec, toGather = true)
              groupby.fold(List[Any]()) { x => groupbyClause(x, rec) }

              // visit having and orderby to collect necessary dataset
              having foreach { x => havingClause(x, rec) }
              orderby foreach { x => orderbyClause(x, rec) }

              hasResult = true
            }
          }
          if (hasResult) {
            BinaryProjection(id, avro.avroEncode(projection, meta.projectionSchema.head).get)
          } else {
            RemoveProjection(id)
          }

        } else {

          // aggregate function can not be applied in WhereClause, so we can decide here

          // we should evaluate selectClause first to get all alias ready.
          selectClause(select, record, toGather = false)
          val whereCond = where.fold(true) { x => whereClause(x, record) }
          if (whereCond) {
            selectClause(select, record, toGather = true)
            groupby.fold(List[Any]()) { x => groupbyClause(x, record) }

            // visit having and orderby to collect necessary dataset
            having foreach { x => havingClause(x, record) }
            orderby foreach { x => orderbyClause(x, record) }

            BinaryProjection(id, avro.avroEncode(projection, meta.projectionSchema.head).get)
          } else {
            RemoveProjection(id) // an empty data may be used to COUNT, but null won't
          }
        }

      case _ => throw new RuntimeException("Applicable on SelectStatement only")
    }
  }

  override def valueOfRecord(attrs: List[String], record: IndexedRecord, toGather: Boolean): Any = {
    if (enterGather && toGather && attrs.headOption != JPQLEvaluator.SOME_ID) {
      var paths = attrs
      var currValue: Any = record
      var valueSchema = record.getSchema

      var currGather: Any = projection
      var gatherSchema = projection.getSchema

      while (paths.nonEmpty) {
        val path = paths.head
        paths = paths.tail

        currValue match {
          case fieldRec: IndexedRecord =>
            // Note: value's schema and field is not same as gather's,
            // since later is projection only
            val valueField = valueSchema.getField(path)
            valueSchema = valueField.schema.getType match {
              case Schema.Type.RECORD => valueField.schema
              case Schema.Type.UNION  => avro.getNonNullOfUnion(valueField.schema)
              case Schema.Type.ARRAY  => valueField.schema // TODO should be ArrayField ?
              case Schema.Type.MAP    => valueField.schema // TODO should be MapKeyField/MapValueField ?
              case _                  => valueField.schema
            }
            val gatherField = gatherSchema.getField(path)
            gatherSchema = gatherField.schema.getType match {
              case Schema.Type.RECORD => gatherField.schema
              case Schema.Type.UNION  => avro.getNonNullOfUnion(gatherField.schema)
              case Schema.Type.ARRAY  => gatherField.schema // TODO should be ArrayField ?
              case Schema.Type.MAP    => gatherField.schema // TODO should be MapKeyField/MapValueField ?
              case _                  => gatherField.schema
            }

            currValue = fieldRec.get(valueField.pos)

            valueField.schema.getType match {
              case Schema.Type.RECORD =>
                if (paths.isEmpty) { // at tail, put clone?
                  currGather.asInstanceOf[IndexedRecord].put(gatherField.pos, currValue)
                  currGather = currValue
                } else {
                  val rec = currGather.asInstanceOf[IndexedRecord].get(gatherField.pos) match {
                    case null => new Record(gatherField.schema)
                    case x    => x
                  }
                  currGather.asInstanceOf[IndexedRecord].put(gatherField.pos, rec)
                  currGather = rec
                }

              case Schema.Type.ARRAY =>
                if (paths.isEmpty) { // at tail, put clone?
                  currValue match {
                    case xs: java.util.Collection[_] =>
                      currGather.asInstanceOf[IndexedRecord].put(gatherField.pos, currValue)
                    case x =>
                      // may access a record flat view's collection field value
                      val arr = currGather.asInstanceOf[IndexedRecord].get(gatherField.pos) match {
                        case null                        => avro.newGenericArray(0, valueSchema)
                        case xs: java.util.Collection[_] => xs
                        case wrong                       => throw JPQLRuntimeException(wrong, "is not a avro array: " + path)
                      }
                      avro.addArray(arr, x)
                      currGather.asInstanceOf[IndexedRecord].put(gatherField.pos, arr)
                  }
                  currGather = currValue
                } else {
                  val arr = currGather.asInstanceOf[IndexedRecord].get(gatherField.pos) match {
                    case null                        => avro.newGenericArray(0, valueSchema)
                    case xs: java.util.Collection[_] => xs
                  }
                  currGather.asInstanceOf[IndexedRecord].put(gatherField.pos, arr)
                  currGather = arr
                }

              case Schema.Type.MAP =>
                if (paths.isEmpty) { // at tail, put clone?
                  currValue match {
                    case xs: java.util.Map[String, _] @unchecked =>
                      currGather.asInstanceOf[IndexedRecord].put(gatherField.pos, currValue)
                    case x: java.util.Map.Entry[String, _] @unchecked =>
                      // may access a record flat view's collection field value
                      val map = currGather.asInstanceOf[IndexedRecord].get(gatherField.pos) match {
                        case null                                      => new java.util.HashMap[String, Any]()
                        case xs: java.util.Map[String, Any] @unchecked => xs
                        case wrong                                     => throw JPQLRuntimeException(wrong, "is not a avro map: " + path)
                      }
                      map.put(x.getKey, x.getValue)
                      currGather.asInstanceOf[IndexedRecord].put(gatherField.pos, map)
                  }
                  currGather = currValue
                } else {
                  val map = currGather.asInstanceOf[IndexedRecord].get(gatherField.pos) match {
                    case null => new java.util.HashMap[String, Any]()
                    case xs   => xs.asInstanceOf[java.util.HashMap[String, Any]]
                  }
                  currGather.asInstanceOf[IndexedRecord].put(gatherField.pos, map)
                  currGather = map
                }

              case _ =>
                // TODO when currGather is map or array
                currGather.asInstanceOf[IndexedRecord].put(gatherField.pos, currValue)
                currGather = currValue

            }

          case arr: java.util.Collection[_]             => throw JPQLRuntimeException(currValue, "is an avro array when fetch its attribute: " + path) // TODO
          case map: java.util.Map[String, _] @unchecked => throw JPQLRuntimeException(currValue, "is an avro map when fetch its attribute: " + path) // TODO
          case null                                     => throw JPQLRuntimeException(currValue, "is null when fetch its attribute: " + path)
          case _                                        => throw JPQLRuntimeException(currValue, "is not a record when fetch its attribute: " + path)
        }
      } // end while

      currValue
    } else {
      super.valueOfRecord(attrs, record, toGather = false)
    }
  }

}
