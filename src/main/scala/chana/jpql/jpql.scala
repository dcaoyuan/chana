package chana

import chana.jpql.nodes.DeleteStatement
import chana.jpql.nodes.InsertStatement
import chana.jpql.nodes.JPQLParser
import chana.jpql.nodes.SelectStatement
import chana.jpql.nodes.Statement
import chana.jpql.nodes.UpdateStatement
import chana.schema.DistributedSchemaBoard
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import scala.util.Failure
import scala.util.Success
import scala.util.Try

package object jpql {
  val jsonNodeFacatory = org.codehaus.jackson.node.JsonNodeFactory.instance

  sealed trait JPQLMeta {
    def stmt: Statement
    def entity: String
    def asToEntity: Map[String, String]
    def asToJoin: Map[String, List[String]]
    def specifiedIds: List[String]
  }
  final case class JPQLSelect(stmt: SelectStatement, entity: String, asToEntity: Map[String, String], asToJoin: Map[String, List[String]], specifiedIds: List[String], projectionSchema: List[Schema]) extends JPQLMeta
  final case class JPQLDelete(stmt: DeleteStatement, entity: String, asToEntity: Map[String, String], asToJoin: Map[String, List[String]], specifiedIds: List[String]) extends JPQLMeta
  final case class JPQLInsert(stmt: InsertStatement, entity: String, asToEntity: Map[String, String], asToJoin: Map[String, List[String]], specifiedIds: List[String]) extends JPQLMeta
  final case class JPQLUpdate(stmt: UpdateStatement, entity: String, asToEntity: Map[String, String], asToJoin: Map[String, List[String]], specifiedIds: List[String]) extends JPQLMeta

  def parseJPQL(jpqlKey: String, jpql: String): Try[JPQLMeta] = {
    val parser = new JPQLParser()
    try {
      val stmt = parser.parse(jpql)
      val meta = new JPQLMetaEvaluator(jpqlKey, DistributedSchemaBoard).collectMeta(stmt, null)
      Success(meta)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

  def update(id: String, record: IndexedRecord, meta: JPQLUpdate) = {
    Try {
      new JPQLMapperUpdate(id, meta).updateEval(record)
    }
  }

  def insert(id: String, record: IndexedRecord, meta: JPQLInsert) = {
    Try {
      new JPQLMapperInsert(id, meta).insertEval(record)
    }
  }

  def delete(id: String, record: IndexedRecord, meta: JPQLDelete) = {
    Try {
      new JPQLMapperDelete(id, meta).deleteEval(record)
    }
  }

}
