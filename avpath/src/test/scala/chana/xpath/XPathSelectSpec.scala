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

class XPathSelectSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {
  import chana.avro.AvroRecords._

  def evulator() = XPathEvaluator

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
    val e = evulator()
    val stmt = parse(query)
    val res = e.select(record, stmt) map (_.value)
    info("\nSelect:\n" + res)
    res
  }

  "XPath Select" when {

    "query fields" should {
      val record = initAccount()

      var q = "/registerTime"
      select(record, q).head should be(
        10000)

      q = "/lastChargeRecord/time"
      select(record, q).head should be(
        2)

      q = "/devApps"
      select(record, q).head should be(
        record.get("devApps").asInstanceOf[java.util.Map[String, _]])

      q = "/devApps/@a"
      select(record, q).head should be(
        record.get("devApps").asInstanceOf[java.util.Map[String, _]].get("a"))

      q = "/devApps/@a/numBlackApps"
      select(record, q).head should be(
        1)

      q = "/devApps/@*"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        record.get("devApps").asInstanceOf[java.util.Map[String, _]].values.toArray)

      q = "/devApps/numBlackApps"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(1, 2, 3))

      q = "/devApps/@*/numBlackApps"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(1, 2, 3))

      q = "/chargeRecords"
      select(record, q).head should be(
        record.get("chargeRecords"))

      q = "/chargeRecords/time"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(1, 2))

      q = "/chargeRecords[1]"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(0)))

      q = "/chargeRecords[1]/time"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(1))

      q = "/chargeRecords[last()]"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(1)))

      q = "/chargeRecords[last()-1]"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(0)))

    }

    "query fields with position() predicates" should {
      val record = initAccount()

      var q = "/chargeRecords[position() = 2]"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(1)))

      q = "/chargeRecords[position() != 1]"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(1)))

      q = "/chargeRecords[position() >= 2]"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(1)))

      q = "/chargeRecords[position() > 1]"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(1)))

      q = "/chargeRecords[position() < 2]"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(0)))

      q = "/chargeRecords[position() <= 1]"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(0)))

      q = "/chargeRecords[position() <= 2]"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].toArray)

      q = "/chargeRecords[position() > 0]"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].toArray)

      q = "/chargeRecords[position() + 1 > 1]"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].toArray)

      q = "/chargeRecords[position() - 1 + 1 > 1]"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(1)))

      q = "/chargeRecords[-position() >= -1]"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(0)))

      q = "/chargeRecords[-position() -1 + 1 >= -1]"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(0)))

      q = "/chargeRecords[1-position() <= -1]"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(1)))

      q = "/chargeRecords[2 <= position()]"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(1)))

      q = "/chargeRecords[1 <= position() - 1]"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(1)))

      q = "/chargeRecords[position() > 1 and position() <= 2]"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(1)))

      q = "/chargeRecords[position() = 1 or position() = 2]"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].toArray)

    }

    "query fields with more predicates" should {
      val record = initAccount()

      var q = "/chargeRecords[time = 2]"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(1)))

      q = "/chargeRecords[time=2 or amount=100.0]"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].toArray)

      q = "/chargeRecords[time*100 = amount]"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(0)))

      q = "/chargeRecords[time=1]/time"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(1))

      q = "/devApps/@a[numBlackApps = 1]"
      select(record, q).head should be(
        record.get("devApps").asInstanceOf[java.util.Map[String, _]].get("a"))

      q = "/devApps/@a[numBlackApps != 1]"
      select(record, q).head should be(
        ())

      // select map values that meet predicates 
      q = "/devApps/@*[numBlackApps=2]"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(record.get("devApps").asInstanceOf[java.util.Map[String, _]].get("b")))

      // select map entries that meet predicates
      q = "/devApps[numBlackApps=2]"
      val expected = {
        val value = record.get("devApps").asInstanceOf[java.util.Map[String, _]].get("b")
        val res = new java.util.LinkedHashMap[String, Any]()
        res.put("b", value)
        res
      }
      select(record, q).head.asInstanceOf[java.util.Map[String, _]] should be(
        expected)

    }

    "query fields with boolean functions" should {
      val record = initAccount()

      var q = "/devApps/@*[not(numBlackApps=2)]"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(
          record.get("devApps").asInstanceOf[java.util.Map[String, _]].get("a"),
          record.get("devApps").asInstanceOf[java.util.Map[String, _]].get("c")))

      q = "/devApps/@*[not(numBlackApps=2)=true()]"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(
          record.get("devApps").asInstanceOf[java.util.Map[String, _]].get("a"),
          record.get("devApps").asInstanceOf[java.util.Map[String, _]].get("c")))

      q = "/devApps/@*[not(numBlackApps=2)=false()]"
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(
          record.get("devApps").asInstanceOf[java.util.Map[String, _]].get("b")))

    }
  }

}
