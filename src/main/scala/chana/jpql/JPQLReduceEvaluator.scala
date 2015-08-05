package chana.jpql

import akka.event.LoggingAdapter
import chana.jpql.nodes._

final case class WorkingSet(selectedItems: List[Any], orderbys: List[Any])

class JPQLReduceEvaluator(log: LoggingAdapter) extends JPQLEvaluator {

  private var idToDataSet = Array[(String, DataSet)]()
  private var aggrCaches = Map[AggregateExpr, Number]()

  def reset(_idToDataSet: Array[(String, DataSet)]) {
    idToDataSet = _idToDataSet
    aggrCaches = Map()
  }

  def visit(root: Statement, record: Map[String, Any]): WorkingSet = {
    selectedItems = List()

    root match {
      case SelectStatement(select, from, where, groupby, having, orderby) =>

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

  override def aggregateExpr(expr: AggregateExpr, record: Any) = {
    aggrCaches.get(expr) match {
      case Some(x) => x
      case None =>
        // TODO isDistinct
        val value = expr match {
          case AggregateExpr_AVG(isDistinct, expr) =>
            var sum = 0.0
            var count = 0
            var i = 0
            val n = idToDataSet.length
            while (i < n) {
              val dataset = idToDataSet(i)._2
              count += 1
              scalarExpr(expr, dataset.values) match {
                case x: Number => sum += x.doubleValue
                case x         => throw new JPQLRuntimeException(x, "is not a number")
              }
              i += 1
            }
            if (count != 0) sum / count else 0

          case AggregateExpr_MAX(isDistinct, expr) =>
            var max = 0.0
            var i = 0
            val n = idToDataSet.length
            while (i < n) {
              val dataset = idToDataSet(i)._2
              scalarExpr(expr, dataset.values) match {
                case x: Number => max = math.max(max, x.doubleValue)
                case x         => throw new JPQLRuntimeException(x, "is not a number")
              }
              i += 1
            }
            max

          case AggregateExpr_MIN(isDistinct, expr) =>
            var min = 0.0
            var i = 0
            val n = idToDataSet.length
            while (i < n) {
              val dataset = idToDataSet(i)._2
              scalarExpr(expr, dataset.values) match {
                case x: Number => min = math.min(min, x.doubleValue)
                case x         => throw new JPQLRuntimeException(x, "is not a number")
              }
              i += 1
            }
            min

          case AggregateExpr_SUM(isDistinct, expr) =>
            var sum = 0.0
            var i = 0
            val n = idToDataSet.length
            while (i < n) {
              val dataset = idToDataSet(i)._2
              scalarExpr(expr, dataset.values) match {
                case x: Number => sum += x.doubleValue
                case x         => throw new JPQLRuntimeException(x, "is not a number")
              }
              i += 1
            }
            sum

          case AggregateExpr_COUNT(isDistinct, expr) =>
            idToDataSet.length
        }
        aggrCaches += (expr -> value)

        value
    }
  }

}
