package chana.jpql

import chana.jpql.nodes._

final class JPQLMapperEvaluator extends JPQLEvaluator {
  /**
   * Major entrence
   */
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

}
