package chana.jpql

import chana.jpql.nodes.JPQLParser
import chana.jpql.rats.JPQLGrammar
import java.io.StringReader
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import xtc.tree.Node

class JPQLEvaluatorSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {
  import chana.avro.AvroRecords._

  def parse(query: String) = {
    val reader = new StringReader(query)
    val grammar = new JPQLGrammar(reader, "<current>")
    val r = grammar.pJPQL(0)
    val rootNode = r.semanticValue[Node]
    info("\n\n## " + query + " ##")
    val parser = new JPQLParser(rootNode)
    val statement = parser.visitRoot()
    info("\nParsed:\n" + statement)
    statement
  }

  "JPQLEvaluator" when {

    "query fields" should {
      val record = initAccount()
      record.put("registerTime", 10000L)
      var q = "SELECT a.registerTime FROM account a WHERE a.registerTime >= 10000"
      var e = new JPQLEvaluator(parse(q), record)
      e.visit() should be(List(10000))

      q = "SELECT a.registerTime FROM account a WHERE a.registerTime < 10000"
      e = new JPQLEvaluator(parse(q), record)
      e.visit() should be(List())
    }
  }

}
