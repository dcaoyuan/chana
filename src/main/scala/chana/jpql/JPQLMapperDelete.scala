package chana.jpql

import chana.avro
import chana.avro.Deletelog
import chana.jpql.nodes._
import org.apache.avro.generic.GenericRecord

final class JPQLMapperDelete(meta: JPQLDelete) extends JPQLEvaluator {

  protected def asToEntity = meta.asToEntity
  protected def asToJoin = meta.asToJoin

  def deleteEval(stmt: DeleteStatement, record: GenericRecord): Boolean = {
    val entityName = deleteClause(stmt.delete, record)
    Deletelog("/", java.util.Collections.EMPTY_LIST) // TODO changeAction
    entityName.equalsIgnoreCase(record.getSchema.getName) && stmt.where.fold(true) { x => whereClause(x, record) }
  }
}
