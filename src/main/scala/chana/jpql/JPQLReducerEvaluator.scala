package chana.jpql

import akka.event.LoggingAdapter
import chana.jpql.nodes._
import org.apache.avro.generic.GenericData.Record

final case class WorkingSet(selectedItems: List[Any], orderbys: List[Any])

final class JPQLReducerEvaluator(log: LoggingAdapter) extends JPQLEvaluator {

  private var idToDataSet = Map[String, ReducerDataSet]()
  private var aggrCaches = Map[AggregateExpr, Number]()

  def reset(_idToDataSet: Map[String, ReducerDataSet]) {
    idToDataSet = _idToDataSet
    aggrCaches = Map()
  }

  def visitOneRecord(root: Statement, record: Record): WorkingSet = {
    selectedItems = List()
    root match {
      case SelectStatement(select, from, where, groupby, having, orderby) =>
        // collect aliases
        fromClause(from, record)

        val havingCond = having.fold(true) { x => havingClause(x, record) }
        if (havingCond) {
          selectClause(select, record)

          val orderbys = orderby.fold(List[Any]()) { x => orderbyClause(x, record) }

          WorkingSet(selectedItems.reverse, orderbys)
        } else {
          null
        }

      case UpdateStatement(update, set, where) => null // NOT YET
      case DeleteStatement(delete, where)      => null // NOT YET
    }
  }

  override def selectExpr(expr: SelectExpr, record: Any) = {
    expr match {
      case SelectExpr_AggregateExpr(expr)   => selectedItems ::= aggregateExpr(expr, record)
      case SelectExpr_ScalarExpr(expr)      => selectedItems ::= scalarExpr(expr, record)
      case SelectExpr_OBJECT(expr)          => selectObjects ::= varAccessOrTypeConstant(expr, record)
      case SelectExpr_ConstructorExpr(expr) => selectNewInstances ::= constructorExpr(expr, record)
      case SelectExpr_MapEntryExpr(expr)    => selectMapEntries ::= mapEntryExpr(expr, record)
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

  override def aggregateExpr(expr: AggregateExpr, record: Any) = {
    aggrCaches.getOrElse(expr, {
      // TODO isDistinct
      val value = expr match {
        case AggregateExpr_AVG(isDistinct, expr) =>
          var sum = 0.0
          var count = 0
          val itr = idToDataSet.iterator
          while (itr.hasNext) {
            val dataset = itr.next._2
            count += 1
            scalarExpr(expr, dataset.projection) match {
              case x: Number => sum += x.doubleValue
              case x         => throw new JPQLRuntimeException(x, "is not a number")
            }
          }
          if (count != 0) sum / count else 0

        case AggregateExpr_MAX(isDistinct, expr) =>
          var max = 0.0
          val itr = idToDataSet.iterator
          while (itr.hasNext) {
            val dataset = itr.next._2
            scalarExpr(expr, dataset.projection) match {
              case x: Number => max = math.max(max, x.doubleValue)
              case x         => throw new JPQLRuntimeException(x, "is not a number")
            }
          }
          max

        case AggregateExpr_MIN(isDistinct, expr) =>
          var min = 0.0
          val itr = idToDataSet.iterator
          while (itr.hasNext) {
            val dataset = itr.next._2
            scalarExpr(expr, dataset.projection) match {
              case x: Number => min = math.min(min, x.doubleValue)
              case x         => throw new JPQLRuntimeException(x, "is not a number")
            }
          }
          min

        case AggregateExpr_SUM(isDistinct, expr) =>
          var sum = 0.0
          val itr = idToDataSet.iterator
          while (itr.hasNext) {
            val dataset = itr.next._2
            scalarExpr(expr, dataset.projection) match {
              case x: Number => sum += x.doubleValue
              case x         => throw new JPQLRuntimeException(x, "is not a number")
            }
          }
          sum

        case AggregateExpr_COUNT(isDistinct, expr) =>
          idToDataSet.size
      }
      aggrCaches += (expr -> value)

      value
    })
  }
}
