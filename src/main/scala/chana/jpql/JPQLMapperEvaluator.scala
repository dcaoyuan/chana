package chana.jpql

import chana.jpql.nodes._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record

/**
 * ProjectionWithId should extends Serializable to got MapperProjection/VoidProjection
 * use custom serializer
 */
sealed trait ProjectionWithId extends Serializable { def id: String }
final case class MapperProjection(id: String, projection: Array[Byte]) extends ProjectionWithId
final case class VoidProjection(id: String) extends ProjectionWithId // used to remove

final class JPQLMapperEvaluator(schema: Schema, projectionSchema: Schema) extends JPQLEvaluator {
  val projection = new Record(projectionSchema)

  def collectProjection(entityId: String, root: Statement, record: Any): ProjectionWithId = {
    root match {
      case SelectStatement(select, from, where, groupby, having, orderby) =>
        fromClause(from, record)

        // Aggregate function can not be applied in WhereClause, so we can decide here
        val whereCond = where.fold(true) { x => whereClause(x, record) }
        if (whereCond) {
          selectClause(select, record)

          groupby.fold(List[Any]()) { x => groupbyClause(x, record) }

          // visit having and orderby to collect necessary dataset
          having foreach { x => havingClause(x, record) }
          orderby foreach { x => orderbyClause(x, record) }

          MapperProjection(entityId, chana.avro.avroEncode(projection, projectionSchema).get)
        } else {
          VoidProjection(entityId) // an empty data may be used to COUNT, but null won't
        }
      case UpdateStatement(update, set, where) => null // NOT YET
      case DeleteStatement(delete, where)      => null // NOT YET
    }
  }

  override def valueOf(qual: String, attrPaths: List[String], record: Any): Any = {
    // TODO in case of record does not contain schema, get entityNames from DistributedSchemaBoard?
    record match {
      case rec: Record =>
        val EntityName = rec.getSchema.getName.toLowerCase
        asToEntity.get(qual) match {
          case Some(EntityName) =>
            var paths = attrPaths
            var currValue: Any = rec

            if (isToCollect) {
              var key = new StringBuilder(qual)
              var currData: Any = projection
              while (paths.nonEmpty) {
                val path = paths.head
                paths = paths.tail

                currValue match {
                  case value: Record =>

                    currValue = value.get(path) match {
                      case v: Record =>
                        currData = if (paths.isEmpty) { // at tail, put r or r clone?
                          currData.asInstanceOf[Record].put(path, v)
                          v
                        } else {
                          val fieldData = currData.asInstanceOf[Record].get(path) match {
                            case null => new Record(currData.asInstanceOf[Record].getSchema.getField(path).schema)
                            case x    => x
                          }
                          currData.asInstanceOf[Record].put(path, fieldData)
                          fieldData
                        }

                        v

                      case v: java.util.Collection[_] =>
                        currData = if (paths.isEmpty) { // at tail, put a or a clone?
                          currData.asInstanceOf[Record].put(path, v)
                          v
                        } else {
                          val fieldData = currData.asInstanceOf[Record].get(path) match {
                            case null => new java.util.ArrayList[Any] //chana.avro.newGenericArray(0, currSchema.getElementType)
                            case x    => x.asInstanceOf[java.util.Collection[_]]
                          }
                          currData.asInstanceOf[Record].put(path, fieldData)
                          fieldData
                        }

                        v

                      case v: java.util.Map[String, _] @unchecked =>
                        currData = if (paths.isEmpty) { // at tail, put m or m clone?
                          currData.asInstanceOf[Record].put(path, v)
                          v
                        } else {
                          val fieldData = currData.asInstanceOf[Record].get(path) match {
                            case null => new java.util.HashMap[String, Any]
                            case x    => x.asInstanceOf[java.util.HashMap[String, Any]]
                          }
                          currData.asInstanceOf[Record].put(path, fieldData)
                          fieldData
                        }

                        v

                      case v =>
                        // TODO when currData is map or array
                        currData.asInstanceOf[Record].put(path, v)
                        currData = v

                        v
                    }
                    key.append(".").append(path)

                  case arr: java.util.Collection[_]             => throw JPQLRuntimeException(currValue, "is not a record when fetch its attribute: " + path) // TODO
                  case map: java.util.Map[String, _] @unchecked => throw JPQLRuntimeException(currValue, "is not a record when fetch its attribute: " + path) // TODO
                  case null                                     => throw JPQLRuntimeException(currValue, "is null when fetch its attribute: " + path)
                  case _                                        => throw JPQLRuntimeException(currValue, "is not a record when fetch its attribute: " + path)
                }
              }
            } else {
              while (paths.nonEmpty) {
                val path = paths.head
                paths = paths.tail

                currValue match {
                  case x: Record                                => currValue = x.get(path)
                  case arr: java.util.Collection[_]             => throw JPQLRuntimeException(currValue, "is not a record when fetch its attribute: " + path) // TODO
                  case map: java.util.Map[String, _] @unchecked => throw JPQLRuntimeException(currValue, "is not a record when fetch its attribute: " + path) // TODO
                  case null                                     => throw JPQLRuntimeException(currValue, "is null when fetch its attribute: " + path)
                  case _                                        => throw JPQLRuntimeException(currValue, "is not a record when fetch its attribute: " + path)
                }
              }
            }
            currValue
          case _ => throw JPQLRuntimeException(qual, "is not an AS alias of entity: " + EntityName)
        }

      case dataset: Map[String, Any] @unchecked =>
        dataset(JPQLEvaluator.keyOf(qual, attrPaths))
    }
  }

  override def pathExprOrVarAccess(expr: PathExprOrVarAccess, record: Any): Any = {
    val qual = qualIdentVar(expr.qual, record)
    val paths = expr.attributes map { x => attribute(x, record) }
    valueOf(qual, paths, record)
  }

  override def pathExpr(expr: PathExpr, record: Any): Any = {
    val qual = qualIdentVar(expr.qual, record)
    val paths = expr.attributes map { x => attribute(x, record) }
    valueOf(qual, paths, record)
  }
}
