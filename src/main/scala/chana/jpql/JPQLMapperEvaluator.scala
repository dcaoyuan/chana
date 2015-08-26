package chana.jpql

import chana.jpql.nodes._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record

sealed trait DataSetWithId {
  def id: String
}
final case class DataSet(id: String, values: Map[String, Any], groupbys: List[Any]) extends DataSetWithId
final case class VoidDataSet(id: String) extends DataSetWithId // used to remove

final class JPQLMapperEvaluator(schema: Schema) extends JPQLEvaluator {
  val data = new Record(schema)

  def collectDataSet(id: String, root: Statement, record: Any): DataSetWithId = {
    root match {
      case SelectStatement(select, from, where, groupby, having, orderby) =>
        fromClause(from, record)

        // Aggregate function can not be applied in WhereClause, so we can decide here
        val whereCond = where.fold(true) { x => whereClause(x, record) }
        if (whereCond) {
          selectClause(select, record)

          val groupbys = groupby.fold(List[Any]()) { x => groupbyClause(x, record) }

          // visit having and orderby to collect necessary dataset
          having foreach { x => havingClause(x, record) }
          orderby foreach { x => orderbyClause(x, record) }

          DataSet(id, dataset, groupbys)
        } else {
          VoidDataSet(id) // an empty data may be used to COUNT, but null won't
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
              var currData: Any = data
              while (paths.nonEmpty) {
                val path = paths.head
                paths = paths.tail

                currValue match {
                  case value: Record =>

                    currValue = value.get(path) match {
                      case r: Record =>
                        if (paths.isEmpty) { // at tail, put r or r clone?
                          currData.asInstanceOf[Record].put(path, r.getSchema)
                        } else {
                          val dataField = currData.asInstanceOf[Record].get(path) match {
                            case null => new Record(r.getSchema)
                            case x    => x
                          }
                          currData.asInstanceOf[Record].put(path, dataField)
                        }
                        currData = r
                        r

                      case a: java.util.Collection[_] =>
                        if (paths.isEmpty) { // at tail, put a or a clone?
                          currData.asInstanceOf[Record].put(path, a)
                        } else {
                          val dataField = currData.asInstanceOf[Record].get(path) match {
                            case null => new java.util.ArrayList[Any] //chana.avro.newGenericArray(0, currSchema.getElementType)
                            case x    => x.asInstanceOf[java.util.Collection[_]]
                          }
                          currData.asInstanceOf[Record].put(path, dataField)
                        }
                        currData = a
                        a

                      case m: java.util.Map[String, _] @unchecked =>
                        if (paths.isEmpty) { // at tail, put m or m clone?
                          currData.asInstanceOf[Record].put(path, m)
                        } else {
                          val dataField = currData.asInstanceOf[Record].get(path) match {
                            case null => new java.util.HashMap[String, Any]
                            case x    => x.asInstanceOf[java.util.HashMap[String, Any]]
                          }
                          currData.asInstanceOf[Record].put(path, dataField)
                        }
                        currData = m
                        m

                      case v =>
                        currData.asInstanceOf[Record].put(path, v) // TODO when currData is map or array
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
              //println("valueOf: " + currData)
              //println("collected record: " + chana.avro.avroEncode(data, schema).get.toString)
              dataset += (key.toString -> currValue)
            } else {
              while (paths.nonEmpty) {
                val path = paths.head
                paths = paths.tail

                currValue match {
                  case x: Record =>
                    currValue = x.get(path)

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
