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

  def insert(meta: JPQLMeta, record: Record) = {
    val jpql = meta.asInstanceOf[JPQLInsert]
    val updateActions = new JPQLMapperInsert(jpql).insertEval(jpql.stmt, record)
    info("\nInsert:\n" + updateActions.map(_.binlog).mkString("\n"))
    updateActions foreach (_.commit())
  }

  def delete(meta: JPQLMeta, record: Record) = {
    val jpql = meta.asInstanceOf[JPQLDelete]
    val updateActions = new JPQLMapperDelete(jpql).deleteEval(jpql.stmt, record)
    info("\nDelete:\n" + updateActions.map(_.binlog).mkString("\n"))
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

    q = "UPDATE account a SET a.lastChargeRecord.time = 200L WHERE a.lastChargeRecord.time <> 100L"
    meta = parse(q)
    update(meta, record)
    record.get("lastChargeRecord").asInstanceOf[Record].get("time") should be(100)

    q = "UPDATE account a SET a.lastChargeRecord.time = 200L WHERE a.lastChargeRecord.time = 100L"
    meta = parse(q)
    update(meta, record)
    record.get("lastChargeRecord").asInstanceOf[Record].get("time") should be(200)

    q = "UPDATE account a SET a.lastChargeRecord = JSON({\"time\": 300e0, \"amount\": 300.0})"
    meta = parse(q)
    update(meta, record)
    record.get("lastChargeRecord").asInstanceOf[Record].get("time") should be(300)
    record.get("lastChargeRecord").asInstanceOf[Record].get("amount") should be(300.0)

    q = "UPDATE account a JOIN a.chargeRecords c SET c.time=400L WHERE INDEX(c) = 2"
    meta = parse(q)
    update(meta, record)
    record.get("chargeRecords").asInstanceOf[java.util.List[Record]].get(1).get("time") should be(400)

    q = "UPDATE account a JOIN a.devApps d SET d.numBlackApps=4 WHERE KEY(d) = 'a'"
    meta = parse(q)
    update(meta, record)
    record.get("devApps").asInstanceOf[java.util.Map[String, Record]].get("a").get("numBlackApps") should be(4)
  }

  "JPQL insert" when {
    val record = initAccount()
    record.put("id", 1)
    record.put("registerTime", 1L)

    var q = "INSERT INTO account (chargeRecords) VALUES (JSON({\"time\": 500e0, \"amount\": 500.0}))"
    var meta = parse(q)
    insert(meta, record)
    record.get("chargeRecords").asInstanceOf[java.util.List[Record]].get(2).get("time") should be(500)

    q = "INSERT INTO account (devApps) VALUES (JSON({\"f\": {\"numBlackApps\": 4}}))"
    meta = parse(q)
    insert(meta, record)
    record.get("devApps").asInstanceOf[java.util.Map[String, Record]].get("f").get("numBlackApps") should be(4)
  }

  "JPQL delete" when {
    val record = initAccount()
    record.put("id", 1)
    record.put("registerTime", 1L)

    val rec_idx1 = record.get("chargeRecords").asInstanceOf[java.util.List[Record]].get(0)
    record.get("chargeRecords").asInstanceOf[java.util.List[Record]].contains(rec_idx1) should be(true)
    record.get("chargeRecords").asInstanceOf[java.util.List[Record]].size should be(2)
    var q = "DELETE FROM account a JOIN a.chargeRecords c WHERE INDEX(c) = 1"
    var meta = parse(q)
    delete(meta, record)
    record.get("chargeRecords").asInstanceOf[java.util.List[Record]].contains(rec_idx1) should be(false)
    record.get("chargeRecords").asInstanceOf[java.util.List[Record]].size should be(1)

    record.get("devApps").asInstanceOf[java.util.Map[String, Record]].keySet.contains("a") should be(true)
    q = "DELETE FROM account a JOIN a.devApps d WHERE KEY(d) = 'a'"
    meta = parse(q)
    delete(meta, record)
    record.get("devApps").asInstanceOf[java.util.Map[String, Record]].keySet.contains("a") should be(false)
  }

}
