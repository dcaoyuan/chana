package chana.jpql

import chana.jpql.nodes.JPQLParser
import chana.jpql.rats.JPQLGrammar
import java.io.StringReader
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.Record
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import xtc.tree.Node

class JPQLEvaluatorSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {
  import chana.avro.AvroRecords._

  def evaluate(query: String, record: Record) = {
    val reader = new StringReader(query)
    val grammar = new JPQLGrammar(reader, "<current>")
    val r = grammar.pJPQL(0)
    val rootNode = r.semanticValue[Node]
    info("\n\n## " + query + " ##")
    val parser = new JPQLParser(rootNode)
    val stmt = parser.visitRoot()
    info("\nParsed:\n" + stmt)
    val e = new JPQLEvaluator(stmt, record)
    val res = e.visit()
    info("\nResult:\n" + res)
    res
  }

  "JPQLEvaluator" when {

    "query fields" should {
      val record = initAccount()
      record.put("registerTime", 10000L)
      record.put("lastLoginTime", 20000L)
      record.put("id", "abcd")

      var q = "SELECT a.registerTime, a.lastLoginTime FROM account a WHERE a.registerTime >= 10000"
      evaluate(q, record) should be(List(10000, 20000))

      q = "SELECT a.registerTime, a.lastLoginTime FROM account a WHERE a.registerTime >= 10000 AND a.lastLoginTime > 20000"
      evaluate(q, record) should be(List())

      q = "SELECT a.registerTime, a.lastLoginTime FROM account a WHERE a.registerTime >= 10000 OR a.lastLoginTime > 20000"
      evaluate(q, record) should be(List(10000, 20000))

      q = "SELECT a.registerTime FROM account a WHERE a.registerTime < 10000"
      evaluate(q, record) should be(List())

      q = "SELECT a.chargeRecords FROM account a WHERE a.registerTime >= 10000"
      evaluate(q, record).asInstanceOf[List[GenericData.Array[_]]](0).size should be(2)

      q = "SELECT a.id FROM account a WHERE a.id = 'abcd'"
      evaluate(q, record) should be(List("abcd"))
    }

    "select with atith calculating and functions" when {
      val record = initAccount()
      record.put("registerTime", 10000L)
      record.put("lastLoginTime", 20000L)
      record.put("id", "abCd")

      var q = "SELECT a.registerTime + 1 FROM account a WHERE a.registerTime + 1 >= 10001"
      evaluate(q, record) should be(List(10001))

      q = "SELECT a.registerTime * 2 FROM account a WHERE a.registerTime * 1 >= 10000"
      evaluate(q, record) should be(List(20000))

      q = "SELECT a.registerTime / 3 FROM account a WHERE a.registerTime + 1 >= 10001"
      evaluate(q, record) should be(List(3333))

      q = "SELECT a.registerTime - 1 FROM account a WHERE a.registerTime + 1 >= 10000"
      evaluate(q, record) should be(List(9999))

      q = "SELECT CONCAT(a.id, '_efgh') FROM account a WHERE CONCAT(a.id, '_efgh') = 'abCd_efgh'"
      evaluate(q, record) should be(List("abCd_efgh"))

      q = "SELECT LENGTH(a.id) FROM account a WHERE LENGTH(a.id) = 4"
      evaluate(q, record) should be(List(4))

      q = "SELECT UPPER(a.id) FROM account a WHERE UPPER(a.id) = 'ABCD'"
      evaluate(q, record) should be(List("ABCD"))

      q = "SELECT LOWER(a.id) FROM account a WHERE LOWER(a.id) = 'abcd'"
      evaluate(q, record) should be(List("abcd"))

      q = "SELECT SUBSTRING(a.id, 2, 2) FROM account a WHERE SUBSTRING(a.id, 2, 2) = 'bC'"
      evaluate(q, record) should be(List("bC"))
    }
  }

}
