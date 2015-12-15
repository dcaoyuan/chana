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

class XPathEvaluatorSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {
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

  def select(query: String, record: Record) = {
    val e = new XPathEvaluator()
    val stmt = parse(query)
    val res = e.select(record, stmt)
    info("\nResult:\n" + res)
    res
  }

  "XPathEvaluator select" when {

    "query fields" should {
      val record = initAccount()
      record.put("registerTime", 10000L)
      record.put("lastLoginTime", 20000L)
      record.put("id", "abcd")

      var q = "/registerTime"
      select(q, record).head should be(
        10000)

      q = "/lastChargeRecord/time"
      select(q, record).head should be(
        2)

      q = "/devApps"
      select(q, record).head should be(
        record.get("devApps").asInstanceOf[java.util.Map[String, _]])

      q = "/devApps/@a"
      select(q, record).head should be(
        record.get("devApps").asInstanceOf[java.util.Map[String, _]].get("a"))

      q = "/devApps/@a/numBlackApps"
      select(q, record).head should be(
        1)

      q = "/devApps/@*"
      select(q, record).head should be(
        record.get("devApps").asInstanceOf[java.util.Map[String, _]].values.toList)

      q = "/chargeRecords"
      select(q, record).head should be(
        record.get("chargeRecords"))

      q = "/chargeRecords[1]"
      select(q, record).head should be(
        List(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(0)))

      q = "/chargeRecords[last()]"
      select(q, record).head should be(
        List(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(1)))

      q = "/chargeRecords[last()-1]"
      select(q, record).head should be(
        List(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(0)))

    }

    "query fields with position() predicates" should {
      val record = initAccount()
      record.put("registerTime", 10000L)
      record.put("lastLoginTime", 20000L)
      record.put("id", "abcd")

      var q = "/chargeRecords[position() = 2]"
      select(q, record).head should be(
        List(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(1)))

      q = "/chargeRecords[position() != 1]"
      select(q, record).head should be(
        List(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(1)))

      q = "/chargeRecords[position() >= 2]"
      select(q, record).head should be(
        List(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(1)))

      q = "/chargeRecords[position() > 1]"
      select(q, record).head should be(
        List(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(1)))

      q = "/chargeRecords[position() < 2]"
      select(q, record).head should be(
        List(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(0)))

      q = "/chargeRecords[position() <= 1]"
      select(q, record).head should be(
        List(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(0)))

      q = "/chargeRecords[position() <= 2]"
      select(q, record).head should be(
        record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].toList)

      q = "/chargeRecords[position() > 0]"
      select(q, record).head should be(
        record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].toList)

      q = "/chargeRecords[position() + 1 > 1]"
      select(q, record).head should be(
        record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].toList)

      q = "/chargeRecords[position() - 1 + 1 > 1]"
      select(q, record).head should be(
        List(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(1)))

      q = "/chargeRecords[-position() >= -1]"
      select(q, record).head should be(
        List(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(0)))

      q = "/chargeRecords[-position() -1 + 1 >= -1]"
      select(q, record).head should be(
        List(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(0)))

      q = "/chargeRecords[1-position() <= -1]"
      select(q, record).head should be(
        List(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(1)))

      q = "/chargeRecords[2 <= position()]"
      select(q, record).head should be(
        List(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(1)))

      q = "/chargeRecords[1 <= position() - 1]"
      select(q, record).head should be(
        List(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(1)))

      q = "/chargeRecords[position() > 1 and position() <= 2]"
      select(q, record).head should be(
        List(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(1)))

      q = "/chargeRecords[position() = 1 or position() = 2]"
      select(q, record).head should be(
        record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].toList)

    }

    "query fields with more predicates" should {
      val record = initAccount()
      record.put("registerTime", 10000L)
      record.put("lastLoginTime", 20000L)
      record.put("id", "abcd")

      var q = "/chargeRecords[time = 2]"
      select(q, record).head should be(
        List(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(1)))

      q = "/chargeRecords[time=2 or amount=100.0]"
      select(q, record).head should be(
        record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].toList)

      q = "/chargeRecords[time*100 = amount]"
      select(q, record).head should be(
        List(record.get("chargeRecords").asInstanceOf[GenericData.Array[_]].get(0)))

      q = "/chargeRecords[time=1]/time"
      select(q, record).head should be(
        List(1))

      q = "/devApps/@a[numBlackApps = 1]"
      select(q, record).head should be(
        record.get("devApps").asInstanceOf[java.util.Map[String, _]].get("a"))

      q = "/devApps/@a[numBlackApps != 1]"
      select(q, record).head should be(
        ())

      // select map values that meet predicates 
      q = "/devApps/@*[numBlackApps=2]"
      select(q, record).head should be(
        List(record.get("devApps").asInstanceOf[java.util.Map[String, _]].get("b")))

      // select map entries that meet predicates
      q = "/devApps[numBlackApps=2]"
      val expected = {
        val itr = record.get("devApps").asInstanceOf[java.util.Map[String, _]].entrySet.iterator
        var entry: java.util.Map.Entry[String, _] = null
        while (itr.hasNext && entry == null) {
          val x = itr.next
          if (x.getKey == "b") {
            entry = x
          }
        }
        entry
      }
      select(q, record).head should be(
        List(expected))

    }

    "query fields with boolean functions" should {
      val record = initAccount()
      record.put("registerTime", 10000L)
      record.put("lastLoginTime", 20000L)
      record.put("id", "abcd")

      var q = "/devApps/@*[not(numBlackApps=2)]"
      select(q, record).head should be(
        List(
          record.get("devApps").asInstanceOf[java.util.Map[String, _]].get("a"),
          record.get("devApps").asInstanceOf[java.util.Map[String, _]].get("c")))

      q = "/devApps/@*[not(numBlackApps=2)=true()]"
      select(q, record).head should be(
        List(
          record.get("devApps").asInstanceOf[java.util.Map[String, _]].get("a"),
          record.get("devApps").asInstanceOf[java.util.Map[String, _]].get("c")))

      q = "/devApps/@*[not(numBlackApps=2)=false()]"
      select(q, record).head should be(
        List(
          record.get("devApps").asInstanceOf[java.util.Map[String, _]].get("b")))

    }

  }

}
