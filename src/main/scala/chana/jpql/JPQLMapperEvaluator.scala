package chana.jpql

import chana.avro.RecordFlatView
import chana.jpql.nodes._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord

/**
 * AvroProjection should extends Serializable to got BinaryProjection/RemoveProjection
 * use custom serializer
 */
sealed trait AvroProjection extends Serializable { def id: String }
final case class BinaryProjection(id: String, projection: Array[Byte]) extends AvroProjection
final case class RemoveProjection(id: String) extends AvroProjection // used to remove

final class JPQLMapperEvaluator(metaData: MetaData) extends JPQLEvaluator {
  private val projection = new Record(metaData.projectionSchema.head) // TODO multiple projections

  protected def asToEntity = metaData.asToEntity
  protected def asToJoin = metaData.asToJoin

  /**
   * Main Entrance
   */
  def gatherProjection(entityId: String, record: Any): AvroProjection = {
    metaData.stmt match {
      case SelectStatement(select, from, where, groupby, having, orderby) =>

        if (asToJoin.nonEmpty) {
          val joinField = asToJoin.head._2.tail.head
          val recordFlatView = new RecordFlatView(record.asInstanceOf[GenericRecord], joinField)
          val itr = recordFlatView.iterator
          var hasResult = false
          while (itr.hasNext) {
            val rec = itr.next
            // aggregate function can not be applied in WhereClause, so we can decide here
            val whereCond = where.fold(true) { x => whereClause(x, rec) }
            if (whereCond) {
              selectClause(select, rec)

              groupby.fold(List[Any]()) { x => groupbyClause(x, rec) }

              // visit having and orderby to collect necessary dataset
              having foreach { x => havingClause(x, rec) }
              orderby foreach { x => orderbyClause(x, rec) }

              hasResult = true
            }
          }
          if (hasResult) {
            BinaryProjection(entityId, chana.avro.avroEncode(projection, metaData.projectionSchema.head).get)
          } else {
            RemoveProjection(entityId)
          }

        } else {

          // aggregate function can not be applied in WhereClause, so we can decide here
          val whereCond = where.fold(true) { x => whereClause(x, record) }
          if (whereCond) {
            selectClause(select, record)

            groupby.fold(List[Any]()) { x => groupbyClause(x, record) }

            // visit having and orderby to collect necessary dataset
            having foreach { x => havingClause(x, record) }
            orderby foreach { x => orderbyClause(x, record) }

            BinaryProjection(entityId, chana.avro.avroEncode(projection, metaData.projectionSchema.head).get)
          } else {
            RemoveProjection(entityId) // an empty data may be used to COUNT, but null won't
          }
        }
      case UpdateStatement(update, set, where) => null // NOT YET
      case DeleteStatement(delete, where)      => null // NOT YET
    }
  }

  override def valueOfRecord(attrPaths: List[String], record: GenericRecord, toGather: Boolean): Any = {
    if (isToGather && toGather) {
      var paths = attrPaths
      var currValue: Any = record

      var currSchema = record.getSchema
      var currGather: Any = projection
      while (paths.nonEmpty) {
        val path = paths.head
        paths = paths.tail

        currValue match {
          case fieldRec: GenericRecord =>
            val field = currSchema.getField(path)
            currSchema = field.schema.getType match {
              case Schema.Type.RECORD => field.schema
              case Schema.Type.UNION  => chana.avro.getFirstNoNullTypeOfUnion(field.schema)
              case Schema.Type.ARRAY  => field.schema // TODO should be ArrayField ?
              case Schema.Type.MAP    => field.schema // TODO should be MapKeyField/MapValueField ?
              case _                  => field.schema
            }

            currValue = fieldRec.get(path)

            field.schema.getType match {
              case Schema.Type.RECORD =>
                currGather = if (paths.isEmpty) { // at tail, put clone?
                  currGather.asInstanceOf[GenericRecord].put(path, currValue)
                  currValue
                } else {
                  val rec = currGather.asInstanceOf[GenericRecord].get(path) match {
                    case null => new Record(field.schema)
                    case x    => x
                  }
                  currGather.asInstanceOf[GenericRecord].put(path, rec)
                  rec
                }

              case Schema.Type.ARRAY =>
                currGather = if (paths.isEmpty) { // at tail, put clone?
                  currValue match {
                    case xs: java.util.Collection[_] =>
                      currGather.asInstanceOf[GenericRecord].put(path, currValue)
                    case x =>
                      // may access a record flat view's collection field value
                      val arr = currGather.asInstanceOf[GenericRecord].get(path) match {
                        case null                        => chana.avro.newGenericArray(0, currSchema)
                        case xs: java.util.Collection[_] => xs
                        case wrong                       => throw JPQLRuntimeException(wrong, "is not a avro array: " + path)
                      }
                      chana.avro.addArray(arr, x)
                      currGather.asInstanceOf[GenericRecord].put(path, arr)
                  }
                  currValue
                } else {
                  val arr = currGather.asInstanceOf[GenericRecord].get(path) match {
                    case null                        => chana.avro.newGenericArray(0, currSchema)
                    case xs: java.util.Collection[_] => xs
                  }
                  currGather.asInstanceOf[GenericRecord].put(path, arr)
                  arr
                }

              case Schema.Type.MAP =>
                currGather = if (paths.isEmpty) { // at tail, put clone?
                  currValue match {
                    case xs: java.util.Map[String, _] @unchecked =>
                      currGather.asInstanceOf[GenericRecord].put(path, currValue)
                    case x: java.util.Map.Entry[String, _] @unchecked =>
                      // may access a record flat view's collection field value
                      val map = currGather.asInstanceOf[GenericRecord].get(path) match {
                        case null                                      => new java.util.HashMap[String, Any]()
                        case xs: java.util.Map[String, Any] @unchecked => xs
                        case wrong                                     => throw JPQLRuntimeException(wrong, "is not a avro map: " + path)
                      }
                      map.put(x.getKey, x.getValue)
                      currGather.asInstanceOf[GenericRecord].put(path, map)
                  }
                  currValue
                } else {
                  val map = currGather.asInstanceOf[GenericRecord].get(path) match {
                    case null => new java.util.HashMap[String, Any]()
                    case xs   => xs.asInstanceOf[java.util.HashMap[String, Any]]
                  }
                  currGather.asInstanceOf[GenericRecord].put(path, map)
                  map
                }

              case _ =>
                // TODO when currGather is map or array
                currGather.asInstanceOf[GenericRecord].put(path, currValue)
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
      super.valueOfRecord(attrPaths, record, toGather = false)
    }
  }

}
