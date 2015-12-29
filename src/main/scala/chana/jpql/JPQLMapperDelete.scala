package chana.jpql

import chana.avro
import chana.avro.Deletelog
import chana.jpql.nodes._
import org.apache.avro.generic.GenericRecord

final class JPQLMapperDelete(meta: JPQLDelete) extends JPQLEvaluator {

  protected def asToEntity = meta.asToEntity
  protected def asToJoin = meta.asToJoin

  def deleteEval(stmt: DeleteStatement, record: GenericRecord) = {
    var toDeletes = List[GenericRecord]()
    if (asToJoin.nonEmpty) {
      val joinField = asToJoin.head._2.tail.head
      val recordFlatView = new avro.RecordFlatView(record.asInstanceOf[GenericRecord], joinField)
      val flatRecs = recordFlatView.iterator

      while (flatRecs.hasNext) {
        val rec = flatRecs.next
        val whereCond = stmt.where.fold(true) { x => whereClause(x, rec) }
        if (whereCond) {
          toDeletes ::= rec
        }
      }
    } else {
      val whereCond = stmt.where.fold(true) { x => whereClause(x, record) }
      if (whereCond) {
        toDeletes ::= record
      }
    }

    if (toDeletes.nonEmpty) {
      stmt.attributes match {
        case Some(x) =>
          val fields = attributesClause(x, record).map { attr => record.getSchema.getField(attr) }.iterator
        case None =>
          Deletelog("/", java.util.Collections.EMPTY_LIST)
      }
    }

    false // TODO
  }
}
