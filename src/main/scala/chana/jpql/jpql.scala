package chana

import org.apache.avro.generic.IndexedRecord
import scala.util.Try

package object jpql {
  val jsonNodeFacatory = org.codehaus.jackson.node.JsonNodeFactory.instance

  def update(data: IndexedRecord, meta: JPQLUpdate) = {
    Try {
      new JPQLMapperUpdate(meta).updateEval(meta.stmt, data)
    }
  }

  def insert(data: IndexedRecord, meta: JPQLInsert) = {
    Try {
      new JPQLMapperInsert(meta).insertEval(meta.stmt, data)
    }
  }

  def delete(data: IndexedRecord, meta: JPQLDelete) = {
    Try {
      new JPQLMapperDelete(meta).deleteEval(meta.stmt, data)
    }
  }

}
