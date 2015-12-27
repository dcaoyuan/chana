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
    //info("meta:\n" + meta)
    meta
  }

  def update(meta: JPQLMeta, record: Record) = {
    val jpql = meta.asInstanceOf[JPQLUpdate]
    val updateActions = new JPQLMapperUpdate(jpql).updateEval(jpql.stmt, record).flatten
    info("\nUpdate:\n" + updateActions.map(_.binlog).mkString("\n"))
    updateActions foreach (_.commit())
  }

  "JPQL update" when {
    val record = initAccount()
    record.put("id", 1)
    record.put("registerTime", 1L)

    var q = "UPDATE account a SET a.registerTime = 100L"
    var meta = parse(q)
    update(meta, record)
    record.get("registerTime") should be(100)

    q = "UPDATE account a SET a.registerTime = 1000L WHERE a.registerTime > 200L "
    meta = parse(q)
    update(meta, record)
    record.get("registerTime") should be(100)

    q = "UPDATE account a SET a.registerTime = 1000L WHERE a.registerTime < 200L "
    meta = parse(q)
    update(meta, record)
    record.get("registerTime") should be(1000)

    q = "UPDATE account a SET a.lastChargeRecord.time = 100L"
    meta = parse(q)
    update(meta, record)
    record.get("lastChargeRecord").asInstanceOf[Record].get("time") should be(100)

    q = "UPDATE account a SET a.lastChargeRecord.time = 200L where a.lastChargeRecord.time <> 100L"
    meta = parse(q)
    update(meta, record)
    record.get("lastChargeRecord").asInstanceOf[Record].get("time") should be(100)

    q = "UPDATE account a SET a.lastChargeRecord.time = 200L where a.lastChargeRecord.time = 100L"
    meta = parse(q)
    update(meta, record)
    record.get("lastChargeRecord").asInstanceOf[Record].get("time") should be(200)

    q = "UPDATE account a SET a.lastChargeRecord = JSON({\"time\": 300e0, \"amount\": 300.0})"
    meta = parse(q)
    update(meta, record)
    record.get("lastChargeRecord").asInstanceOf[Record].get("time") should be(300)
    record.get("lastChargeRecord").asInstanceOf[Record].get("amount") should be(300.0)
  }

}
