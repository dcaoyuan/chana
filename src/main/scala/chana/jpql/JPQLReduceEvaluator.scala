package chana.jpql

import akka.event.LoggingAdapter
import chana.jpql.nodes._

class JPQLReduceEvaluator(log: LoggingAdapter) extends JPQLEvaluator {

  private var idToValues = Map[String, Map[String, Any]]()
  private var aggrCaches = Map[AggregateExpr, Number]()

  def visit(root: Statement, record: Map[String, Any], _idToValues: Map[String, Map[String, Any]]) = {
    selectScalars = List()
    idToValues = _idToValues
    aggrCaches = Map()

    root match {
      case SelectStatement(select, from, where, groupby, having, orderby) =>
        selectClause(select, record)
        selectScalars = selectScalars.reverse

        groupby match {
          case Some(x) => groupbyClause(x, record)
          case None    =>
        }
        having match {
          case Some(x) => havingClause(x, record)
          case None    =>
        }
        orderby match {
          case Some(x) => orderbyClause(x, record)
          case None    =>
        }

        selectScalars
      case _ => List()
    }
  }

  override def selectExpr(expr: SelectExpr, record: Any) = {
    expr match {
      case SelectExpr_AggregateExpr(expr)   => selectScalars ::= aggregateExpr(expr, record)
      case SelectExpr_ScalarExpr(expr)      => selectScalars ::= scalarExpr(expr, record)
      case SelectExpr_OBJECT(expr)          => selectObjects ::= varAccessOrTypeConstant(expr, record)
      case SelectExpr_ConstructorExpr(expr) => selectNewInstances ::= constructorExpr(expr, record)
      case SelectExpr_MapEntryExpr(expr)    => selectMapEntries ::= mapEntryExpr(expr, record)
    }
  }

  override def aggregateExpr(expr: AggregateExpr, record: Any) = {
    aggrCaches.get(expr) match {
      case Some(x) => x
      case None =>
        val value = expr match {
          case AggregateExpr_AVG(isDistinct, expr) =>
            var sum = 0.0
            var count = 0
            for { (id, values) <- idToValues if values.nonEmpty } {
              count += 1
              scalarExpr(expr, values) match {
                case x: Number => sum += x.doubleValue
                case x         => throw new JPQLRuntimeException(x, "is not a number")
              }
            }
            if (count != 0) sum / count else 0

          case AggregateExpr_MAX(isDistinct, expr) =>
            var max = 0.0
            for { (id, values) <- idToValues if values.nonEmpty } {
              scalarExpr(expr, values) match {
                case x: Number => max = math.max(max, x.doubleValue)
                case x         => throw new JPQLRuntimeException(x, "is not a number")
              }
            }
            max

          case AggregateExpr_MIN(isDistinct, expr) =>
            var min = 0.0
            for { (id, values) <- idToValues if values.nonEmpty } {
              scalarExpr(expr, values) match {
                case x: Number => min = math.min(min, x.doubleValue)
                case x         => throw new JPQLRuntimeException(x, "is not a number")
              }
            }
            min

          case AggregateExpr_SUM(isDistinct, expr) =>
            var sum = 0.0
            for { (id, values) <- idToValues if values.nonEmpty } {
              scalarExpr(expr, values) match {
                case x: Number => sum += x.doubleValue
                case x         => throw new JPQLRuntimeException(x, "is not a number")
              }
            }
            sum

          case AggregateExpr_COUNT(isDistinct, expr) =>
            var count = 0
            for { (id, values) <- idToValues if values.nonEmpty } {
              count += 1
            }
            count
        }
        aggrCaches += (expr -> value)

        value
    }
  }

}
