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
    // for the signature of method: <T> T semanticValue(), we have to call
    // with at least of Any to avoid:
    //   xtc.tree.GNode$Fixed1 cannot be cast to scala.runtime.Nothing$
    val rootNode = r.semanticValue[Node]
    info("\n\n## " + query + " ##\n\n" + rootNode)
    val parser = new JPQLParser(rootNode)
    val statement = parser.visitRoot()
    info("\nParsed:\n" + statement)
    statement
  }

  "JPQLEvaluator" when {

    "query fields" should {
      val record = initAccount()
      record.put("registerTime", 10000L)
      val q = "SELECT a.registerTime FROM account a"
      val stmt = parse(q)
      val e = new JPQLEvaluator(stmt, record)
      e.visit() should be(List(10000))
    }
  }

}
