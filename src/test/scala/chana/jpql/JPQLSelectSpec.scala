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

class JPQLSelectSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {
  import chana.avro.AvroRecords._

  def eval(query: String, record: Record) = {
    val reader = new StringReader(query)
    val grammar = new JPQLGrammar(reader, "")
    val r = grammar.pJPQL(0)
    val rootNode = r.semanticValue[Node]
    info("\n\n## " + query + " ##")

    // now let's do Parsing
    val parser = new JPQLParser()
    val stmt = parser.parse(query)
    info("\nParsed:\n" + stmt)

    val e = new JPQLEvaluator {
      def id = "abCd1"
      protected var asToEntity = Map[String, String]()
      protected var asToJoin = Map[String, List[String]]()
      override protected def addAsToEntity(as: String, entity: String) = asToEntity += (as -> entity)
      override protected def addAsToJoin(as: String, joinPath: List[String]) = asToJoin += (as -> joinPath)
    }
    val res = e.simpleEval(stmt, record)
    info("\nResult:\n" + res)
    res
  }

  "JPQL select" when {

    "query fields" should {
      val record = initAccount()

      var q = "SELECT a.registerTime, a.lastLoginTime FROM account a WHERE a.registerTime >= 10000"
      eval(q, record) should be(List(10000, 20000))

      q = "SELECT a.registerTime, a.lastLoginTime FROM account a WHERE a.registerTime >= 10000 AND a.lastLoginTime > 20000"
      eval(q, record) should be(List())

      q = "SELECT a.registerTime, a.lastLoginTime FROM account a WHERE a.registerTime >= 10000 OR a.lastLoginTime > 20000"
      eval(q, record) should be(List(10000, 20000))

      q = "SELECT a.registerTime FROM account a WHERE a.registerTime <> 0"
      eval(q, record) should be(List(10000))

      q = "SELECT a.registerTime FROM account a WHERE a.registerTime < 10000"
      eval(q, record) should be(List())

      q = "SELECT a.chargeRecords FROM account a WHERE a.registerTime >= 10000"
      eval(q, record).asInstanceOf[List[GenericData.Array[_]]](0).size should be(2)

      q = "SELECT a.id FROM account a WHERE a.id = 'abCd1'"
      eval(q, record) should be(List("abCd1"))
    }

    "select with atith calculating and functions" when {
      val record = initAccount()
      record.put("registerTime", 10000L)
      record.put("lastLoginTime", 20000L)
      record.put("balance", 100.0)

      var q = "SELECT a.registerTime + 1 FROM account a WHERE a.registerTime + 1 >= 10001"
      eval(q, record) should be(List(10001))

      q = "SELECT a.registerTime * 2 FROM account a WHERE a.registerTime * 1 >= 10000"
      eval(q, record) should be(List(20000))

      q = "SELECT a.registerTime / 3 FROM account a WHERE a.registerTime + 1 >= 10001"
      eval(q, record) should be(List(3333))

      q = "SELECT a.registerTime - 1 FROM account a WHERE a.registerTime + 1 >= 10000"
      eval(q, record) should be(List(9999))

      q = "SELECT a.balance / 3 FROM account a WHERE a.balance / 3 < 33.4"
      eval(q, record) should be(List(33.333333333333336))

      q = "SELECT -a.balance FROM account a WHERE -a.balance = -100"
      eval(q, record) should be(List(-100.0))

      q = "SELECT ABS(-a.balance) FROM account a WHERE ABS(-a.balance) = 100"
      eval(q, record) should be(List(100.0))

      q = "SELECT SQRT(a.balance) FROM account a WHERE SQRT(a.balance) = 10"
      eval(q, record) should be(List(10.0))

      q = "SELECT CONCAT(a.id, '_efgh') FROM account a WHERE CONCAT(a.id, '_efgh') = 'abCd1_efgh'"
      eval(q, record) should be(List("abCd1_efgh"))

      q = "SELECT LENGTH(a.id) FROM account a WHERE LENGTH(a.id) = 5"
      eval(q, record) should be(List(5))

      q = "SELECT UPPER(a.id) FROM account a WHERE UPPER(a.id) = 'ABCD1'"
      eval(q, record) should be(List("ABCD1"))

      q = "SELECT LOWER(a.id) FROM account a WHERE LOWER(a.id) = 'abcd1'"
      eval(q, record) should be(List("abcd1"))

      q = "SELECT SUBSTRING(a.id, 2, 2) FROM account a WHERE SUBSTRING(a.id, 2, 2) = 'bC'"
      eval(q, record) should be(List("bC"))
    }
  }

}
