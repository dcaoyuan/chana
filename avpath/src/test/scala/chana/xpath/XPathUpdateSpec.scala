package chana.xpath

import chana.avro
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

  def evaluator() = XPathEvaluator

  def parse(query: String) = {
    val reader = new StringReader(query)
    val grammar = new XPathGrammar(reader, "")
    val r = grammar.pXPath(0)
    val rootNode = r.semanticValue[Node]
    info("\n\n## " + query + " ##")

    // now let's do Parsing
    val parser = new XPathParser()
    val stmt = parser.parse(query)
    //info("\nParsed:\n" + stmt)
    stmt
  }

  def select(record: Record, query: String) = {
    val e = evaluator()
    val stmt = parse(query)
    val res = e.select(record, stmt) map (_.value)
    //info("\nSelect:\n" + res)
    res
  }

  def update(record: Record, query: String, value: Any) = {
    val e = evaluator()
    val stmt = parse(query)
    val res = e.update(record, stmt, value)
    info("\nUpdate:\n" + res.map(_.binlog).mkString("\n"))
    res foreach { _.commit() }
  }

  def insertJson(record: Record, query: String, value: String) = {
    val e = evaluator()
    val stmt = parse(query)
    val res = e.insertJson(record, stmt, value)
    info("\nInsert:\n" + res.map(_.binlog).mkString("\n"))
    res foreach { _.commit() }
  }

  def insertAllJson(record: Record, query: String, value: String) = {
    val e = evaluator()
    val stmt = parse(query)
    val res = e.insertAllJson(record, stmt, value)
    info("\nInsertAll:\n" + res.map(_.binlog).mkString("\n"))
    res foreach { _.commit() }
  }

  def delete(record: Record, query: String) = {
    val e = evaluator()
    val stmt = parse(query)
    val res = e.delete(record, stmt)
    info("\nDelete:\n" + res.map(_.binlog).mkString("\n"))
    res foreach { _.commit() }
  }

  def clear(record: Record, query: String) = {
    val e = evaluator()
    val stmt = parse(query)
    val res = e.clear(record, stmt)
    info("\nClear:\n" + res.map(_.binlog).mkString("\n"))
    res foreach { _.commit() }
  }

  "XPath Update" when {

    "update field" should {
      val record = initAccount()

      var q = "/registerTime"
      update(record, q, 8L)
      select(record, q).head should be(
        8)

      q = "/lastChargeRecord/time"
      update(record, q, 8L)
      select(record, q).head should be(
        8)

      q = "/devApps/numBlackApps"
      update(record, q, 88)
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(88, 88, 88))

      q = "/devApps/@*/numBlackApps"
      update(record, q, 888)
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(888, 888, 888))

      q = "/devApps/@a/numBlackApps"
      update(record, q, 8)
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(8))

      q = "/chargeRecords[2]/time"
      update(record, q, 88L)
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(88))

      q = "/chargeRecords[position()>0]/time"
      update(record, q, 888L)
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(888, 888))

      q = "/chargeRecords/time"
      update(record, q, 8888L)
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(8888, 8888))

    }

    "insert field" should {
      val record = initAccount()

      var q = "/devApps"
      var json = "{'e' : {}}"
      var value = avro.FromJson.fromJsonString("{}", appInfoSchema, false)
      insertJson(record, q, json)
      select(record, q).head.asInstanceOf[java.util.Map[String, _]].get("e") should be(
        value)

      q = "/chargeRecords"
      json = "{'time': 4, 'amount': -4.0}"
      value = avro.FromJson.fromJsonString(json, chargeRecordSchema, false)
      insertJson(record, q, json)
      select(record, q).head.asInstanceOf[java.util.Collection[_]].contains(value) should be(
        true)

    }

    "insertAll field" should {
      val record = initAccount()
      record.put("registerTime", 10000L)
      record.put("lastLoginTime", 20000L)
      record.put("id", "abcd")

      var q = "/devApps"
      var json = "{'g' : {}, 'h' : {'numBlackApps': 10}}"
      var value = avro.FromJson.fromJsonString("{'numBlackApps': 10}", appInfoSchema, false)
      insertAllJson(record, q, json)
      select(record, q).head.asInstanceOf[java.util.Map[String, _]].get("h") should be(
        value)

      q = "/chargeRecords"
      json = "[{'time': 3, 'amount': -5.0}, {'time': 4, 'amount': -6.0}]"
      value = avro.FromJson.fromJsonString(json, chargeRecordsSchema, false)
      insertAllJson(record, q, json)
      select(record, q).head.asInstanceOf[java.util.Collection[_]].containsAll(value.asInstanceOf[java.util.Collection[_]]) should be(
        true)

    }

    "delete field" should {
      val record = initAccount()

      var q = "/devApps/@a"
      delete(record, q)
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array(null))

      q = "/chargeRecords[2]"
      delete(record, q)
      select(record, q).head.asInstanceOf[java.util.Collection[_]].toArray should be(
        Array())

    }

    "clear field" should {
      val record = initAccount()

      var q = "/devApps"
      clear(record, q)
      select(record, q).head.asInstanceOf[java.util.Map[String, _]].size should be(
        0)

      q = "/chargeRecords"
      clear(record, q)
      select(record, q).head.asInstanceOf[java.util.Collection[_]].size should be(
        0)

    }
  }

}
