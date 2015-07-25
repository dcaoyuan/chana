package chana.jpql

import chana.jpql.nodes.JPQLParser
import chana.jpql.rats.JPQLGrammar
import java.io.StringReader
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

      var q = "SELECT a.registerTime, a.lastLoginTime FROM account a WHERE a.registerTime >= 10000"
      evaluate(q, record) should be(List(10000, 20000))

      q = "SELECT a.registerTime, a.lastLoginTime FROM account a WHERE a.registerTime >= 10000 AND a.lastLoginTime > 20000"
      evaluate(q, record) should be(List())

      q = "SELECT a.registerTime, a.lastLoginTime FROM account a WHERE a.registerTime >= 10000 OR a.lastLoginTime > 20000"
      evaluate(q, record) should be(List(10000, 20000))

      q = "SELECT a.registerTime FROM account a WHERE a.registerTime < 10000"
      evaluate(q, record) should be(List())
    }
  }

}
