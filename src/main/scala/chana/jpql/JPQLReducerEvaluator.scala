package chana.jpql

import akka.event.LoggingAdapter
import chana.avro.RecordFlatView
import chana.jpql.nodes._
import org.apache.avro.generic.GenericRecord

final case class WorkingSet(selectedItems: List[Any], orderbys: List[Any])

final class JPQLReducerEvaluator(metaData: MetaData, log: LoggingAdapter) extends JPQLEvaluator {

  protected def asToEntity = metaData.asToEntity
  protected def asToJoin = metaData.asToJoin

  private var idToProjection = Iterable[RecordProjection]()
  private var aggrCaches = Map[AggregateExpr, Number]()

  def reset(_idToProjection: Iterable[RecordProjection]) {
    idToProjection = _idToProjection
    aggrCaches = Map()
  }

  def visitGroupbys(record: GenericRecord): List[Any] = {
    metaData.stmt match {
      case SelectStatement(select, from, where, groupby, having, orderby) =>
        groupby.fold(List[Any]()) { x => groupbyClause(x, record) }

      case UpdateStatement(update, set, where) => null // NOT YET
      case DeleteStatement(delete, where)      => null // NOT YET
    }
  }

  def visitOneRecord(record: GenericRecord): List[WorkingSet] = {
    selectedItems = List()
    metaData.stmt match {
      case SelectStatement(select, from, where, groupby, having, orderby) =>

        if (metaData.asToJoin.nonEmpty) {
          val joinField = metaData.asToJoin.head._2.tail.head
          val recordFlatView = new RecordFlatView(record.asInstanceOf[GenericRecord], joinField)
          val itr = recordFlatView.iterator

          var res = List[WorkingSet]()
          while (itr.hasNext) {
            val rec = itr.next
            val havingCond = having.fold(true) { x => havingClause(x, record) }
            if (havingCond) {
              selectClause(select, rec)

              val orderbys = orderby.fold(List[Any]()) { x => orderbyClause(x, rec) }

              res ::= WorkingSet(selectedItems.reverse, orderbys)
            }
          }
          res

        } else {

          val havingCond = having.fold(true) { x => havingClause(x, record) }
          if (havingCond) {
            selectClause(select, record)

            val orderbys = orderby.fold(List[Any]()) { x => orderbyClause(x, record) }

            List(WorkingSet(selectedItems.reverse, orderbys))
          } else {
            List()
          }
        }

      case UpdateStatement(update, set, where) => null // NOT YET
      case DeleteStatement(delete, where)      => null // NOT YET
    }
  }

  override def aggregateExpr(expr: AggregateExpr, record: Any) = {
    aggrCaches.getOrElse(expr, {
      // TODO isDistinct
      val value = expr match {
        case AggregateExpr_AVG(isDistinct, expr) =>
          var sum = 0.0
          var count = 0
          val itr = idToProjection.iterator
          while (itr.hasNext) {
            val dataset = itr.next
            count += 1
            scalarExpr(expr, dataset.projection) match {
              case x: Number => sum += x.doubleValue
              case x         => throw new JPQLRuntimeException(x, "is not a number")
            }
          }
          if (count != 0) sum / count else 0

        case AggregateExpr_MAX(isDistinct, expr) =>
          var max = 0.0
          val itr = idToProjection.iterator
          while (itr.hasNext) {
            val dataset = itr.next
            scalarExpr(expr, dataset.projection) match {
              case x: Number => max = math.max(max, x.doubleValue)
              case x         => throw new JPQLRuntimeException(x, "is not a number")
            }
          }
          max

        case AggregateExpr_MIN(isDistinct, expr) =>
          var min = 0.0
          val itr = idToProjection.iterator
          while (itr.hasNext) {
            val dataset = itr.next
            scalarExpr(expr, dataset.projection) match {
              case x: Number => min = math.min(min, x.doubleValue)
              case x         => throw new JPQLRuntimeException(x, "is not a number")
            }
          }
          min

        case AggregateExpr_SUM(isDistinct, expr) =>
          var sum = 0.0
          val itr = idToProjection.iterator
          while (itr.hasNext) {
            val dataset = itr.next
            scalarExpr(expr, dataset.projection) match {
              case x: Number => sum += x.doubleValue
              case x         => throw new JPQLRuntimeException(x, "is not a number")
            }
          }
          sum

        case AggregateExpr_COUNT(isDistinct, expr) =>
          idToProjection.size
      }
      aggrCaches += (expr -> value)

      value
    })
  }
}
