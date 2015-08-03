package chana.jpql

import akka.event.LoggingAdapter
import chana.jpql.nodes._

class JPQLReduceEvaluator(root: Statement, log: LoggingAdapter) extends JPQLEvaluator(root, null) {

  override def valueOf(qual: String, attrPaths: List[String]): Any = {

    val x = values(JPQLEvaluator.keyOf(qual, attrPaths))
    log.info("value: {}", x)
    x
  }

  def visit(_values: Map[String, Any]) = {
    selectScalars = List()
    values = _values
    root match {
      case SelectStatement(select, from, where, groupby, having, orderby) =>
        selectClause(select)
        selectScalars = selectScalars.reverse
        log.info("selectScalars: {}", selectScalars)

        groupby match {
          case Some(x) => groupbyClause(x)
          case None    =>
        }
        having match {
          case Some(x) => havingClause(x)
          case None    =>
        }
        orderby match {
          case Some(x) => orderbyClause(x)
          case None    =>
        }

        selectScalars
      case _ => List()
    }
  }
}
