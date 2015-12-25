package chana.jpql

import chana.jpql.nodes.JPQLParser
import chana.jpql.rats.JPQLGrammar
import chana.schema.SchemaBoard
import java.io.StringReader
import org.apache.avro.generic.GenericData.Record
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import xtc.tree.Node

class JPQLUpdateSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {
  import chana.avro.AvroRecords._

  val schemaBoard = new SchemaBoard {
    val entityToSchema = Map("account" -> schema)
    def schemaOf(entityName: String) = entityToSchema.get(entityName)
  }

  def parse(query: String) = {
    val reader = new StringReader(query)
    val grammar = new JPQLGrammar(reader, "")
    val r = grammar.pJPQL(0)
    val rootNode = r.semanticValue[Node]
    info("\n\n## " + query + " ##")

    // now let's do JPQLParsing
    val parser = new JPQLParser()
    val stmt = parser.parse(query)
    //info("\nParsed:\n" + stmt)
    val metaEval = new JPQLMetaEvaluator("2", schemaBoard)
    val meta = metaEval.collectMeta(stmt, null)
    info("meta:\n" + meta)
    meta
  }

  def update(meta: JPQLMeta, record: Record) = {
    val jpql = meta.asInstanceOf[JPQLUpdate]
    new JPQLMapperUpdate(jpql).updateEval(jpql.stmt, record)
  }

  "JPQL update" when {
    val record = initAccount()
    record.put("registerTime", 1L)

    var q = "UPDATE account a SET a.registerTime = 100L"
    val meta = parse(q)
    update(meta, record).head map (_.commit())
    record.get("registerTime") should be(100)
  }

}
