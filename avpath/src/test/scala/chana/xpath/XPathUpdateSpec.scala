package chana.xpath

import chana.xpath.nodes.XPathParser
import chana.xpath.rats.XPathGrammar
import java.io.StringReader
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.Record
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import scala.collection.JavaConversions._
import xtc.tree.Node

class XPathUpdateSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {
  import chana.avro.AvroRecords._

  def parse(query: String) = {
    val reader = new StringReader(query)
    val grammar = new XPathGrammar(reader, "")
    val r = grammar.pXPath(0)
    val rootNode = r.semanticValue[Node]
    info("\n\n## " + query + " ##")

    // now let's do Parsing
    val parser = new XPathParser()
    val stmt = parser.parse(query)
    info("\nParsed:\n" + stmt)
    stmt
  }

  def select(record: Record, query: String) = {
    val e = new XPathEvaluator()
    val stmt = parse(query)
    val res = e.select(record, stmt) map (_.value)
    info("\nSelect:\n" + res)
    res
  }

  def update(record: Record, query: String, value: Any) = {
    val e = new XPathEvaluator()
    val stmt = parse(query)
    val res = e.update(record, stmt, value)
    info("\nUpdate:\n" + res)
    res
  }

  "XPath Update" when {

    "query fields" should {
      val record = initAccount()
      record.put("registerTime", 10000L)
      record.put("lastLoginTime", 20000L)
      record.put("id", "abcd")

      var q = "/registerTime"
      update(record, q, 8) foreach { _.commit() }
      select(record, q).head should be(
        8)

      q = "/lastChargeRecord/time"
      update(record, q, 8) foreach { _.commit() }
      select(record, q).head should be(
        8)

      q = "/devApps/numBlackApps"
      update(record, q, 88) foreach { _.commit() }
      select(record, q).head should be(
        List(88, 88, 88))

      q = "/devApps/@*/numBlackApps"
      update(record, q, 888) foreach { _.commit() }
      select(record, q).head should be(
        List(888, 888, 888))

      q = "/devApps/@a/numBlackApps"
      update(record, q, 8) foreach { _.commit() }
      select(record, q).head should be(
        8)

      q = "/chargeRecords[2]/time"
      update(record, q, 88) foreach { _.commit() }
      select(record, q).head should be(
        List(88))

      q = "/chargeRecords/time"
      update(record, q, 888) foreach { _.commit() }
      select(record, q).head should be(
        List(888, 888))

    }
  }

}
