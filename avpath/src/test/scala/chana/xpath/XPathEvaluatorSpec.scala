package chana.xpath

import chana.xpath.nodes.XPathParser
import chana.xpath.rats.XPathGrammar
import java.io.StringReader
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.Record
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import xtc.tree.Node

class XPathEvaluatorSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {
  import chana.avro.AvroRecords._

  def eval(query: String, record: Record) = {
    val reader = new StringReader(query)
    val grammar = new XPathGrammar(reader, "<current>")
    val r = grammar.pXPath(0)
    val rootNode = r.semanticValue[Node]
    info("\n\n## " + query + " ##")

    // now let's do Parsing
    val parser = new XPathParser()
    val stmt = parser.parse(query)
    info("\nParsed:\n" + stmt)

    val e = new XPathEvaluator()
    val res = e.simpleEval(stmt, record)
    info("\nResult:\n" + res)
    res
  }

  "XPathEvaluator" when {

    "query fields" should {
      val record = initAccount()
      record.put("registerTime", 10000L)
      record.put("lastLoginTime", 20000L)
      record.put("id", "abcd")

      var q = "/registerTime"
      eval(q, record) should be(List(List()))
    }

  }

}
