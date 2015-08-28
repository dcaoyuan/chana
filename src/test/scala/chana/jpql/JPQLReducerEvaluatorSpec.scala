package chana.jpql

import akka.actor.ActorSystem
import akka.testkit.ImplicitSender
import akka.testkit.TestActorRef
import akka.testkit.TestKit
import akka.pattern.ask
import chana.jpql.JPQLReducer.AskReducedResult
import chana.jpql.nodes.JPQLParser
import chana.jpql.nodes.Statement
import chana.jpql.rats.JPQLGrammar
import chana.schema.SchemaBoard
import java.io.StringReader
import org.apache.avro.generic.GenericData.Record
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import scala.util.Success
import xtc.tree.Node
import scala.concurrent.duration._

class JPQLReducerEvaluatorSpec extends TestKit(ActorSystem("ChanaSystem")) with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  import chana.avro.AvroRecords._

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  def parse(query: String) = {
    val reader = new StringReader(query)
    val grammar = new JPQLGrammar(reader, "<current>")
    val r = grammar.pJPQL(0)
    val rootNode = r.semanticValue[Node]
    info("\n\n## " + query + " ##")
    val parser = new JPQLParser(rootNode)
    val stmt = parser.visitRoot()
    //info("\nParsed:\n" + stmt)
    stmt
  }

  def collect(id: String, stmt: Statement, record: Record) = {
    val e = new JPQLMapperEvaluator(record.getSchema)
    val res = e.collectDataSet(id, stmt, record)
    info("\nCollected:\n" + res)
    res
  }

  def records() = {
    for (id <- 0 to 9) yield {
      val record = initAccount()
      record.put("registerTime", id)
      record.put("lastLoginTime", id % 3)
      record.put("id", id.toString)

      val chargeRecord = chargeRecordBuilder.build()
      chargeRecord.put("time", id * 1000)
      chargeRecord.put("amount", id * 100.0)
      record.put("lastChargeRecord", chargeRecord)

      record
    }
  }

  "JPQLReduceEvaluator" must {

    val schemaBoard = new SchemaBoard {
      val entityToSchema = Map("account" -> schema)
      def schemaOf(entityName: String) = entityToSchema.get(entityName)
    }

    "query fields" in {
      val q = "SELECT a.registerTime FROM account a " +
        "WHERE a.registerTime >= 5 ORDER BY a.registerTime"
      val stmt = parse(q)

      val reducer = TestActorRef(JPQLReducer.props("test", stmt))

      records() foreach { record => reducer ! collect(record.get("id").asInstanceOf[String], stmt, record) }

      val f = reducer.ask(AskReducedResult)(2.seconds)
      val Success(result) = f.value.get
      result should be(Array(List(5), List(6), List(7), List(8), List(9)))
    }

    "query deep fields" in {
      val q = "SELECT a.registerTime, a.lastChargeRecord.time FROM account a " +
        "WHERE a.registerTime >= 5 ORDER BY a.registerTime"
      val stmt = parse(q)

      val metaEval = new JPQLMetadataEvaluator("2", schemaBoard)
      val projectionSchema = metaEval.collectMetaSet(stmt, null)
      info("Projection Schema:\n" + projectionSchema)

      val reducer = TestActorRef(JPQLReducer.props("test", stmt))

      records() foreach { record => reducer ! collect(record.get("id").asInstanceOf[String], stmt, record) }

      val f = reducer.ask(AskReducedResult)(2.seconds)
      val Success(result) = f.value.get
      result should be(Array(List(5, 5000), List(6, 6000), List(7, 7000), List(8, 8000), List(9, 9000)))
    }

    "query fields order desc" in {
      val q = "SELECT a.registerTime FROM account a " +
        "WHERE a.registerTime >= 5 ORDER BY a.registerTime DESC"
      val stmt = parse(q)

      val reducer = TestActorRef(JPQLReducer.props("test", stmt))

      records() foreach { record => reducer ! collect(record.get("id").asInstanceOf[String], stmt, record) }

      val f = reducer.ask(AskReducedResult)(2.seconds)
      val Success(result) = f.value.get
      result should be(Array(List(9), List(8), List(7), List(6), List(5)))
    }

    "query fields order by string desc" in {
      val q = "SELECT a.id, a.registerTime FROM account a " +
        "WHERE a.registerTime >= 5 ORDER BY a.id DESC"
      val stmt = parse(q)

      val reducer = TestActorRef(JPQLReducer.props("test", stmt))

      records() foreach { record => reducer ! collect(record.get("id").asInstanceOf[String], stmt, record) }

      val f = reducer.ask(AskReducedResult)(2.seconds)
      val Success(result) = f.value.get
      result should be(Array(List("9", 9), List("8", 8), List("7", 7), List("6", 6), List("5", 5)))
    }

    "query aggregate functions" in {
      val q = "SELECT COUNT(a.id), AVG(a.registerTime), SUM(a.registerTime), MAX(a.registerTime), MIN(a.registerTime) FROM account a " +
        "WHERE a.registerTime >= 5 ORDER BY a.id DESC"
      val stmt = parse(q)

      val reducer = TestActorRef(JPQLReducer.props("test", stmt))

      records() foreach { record => reducer ! collect(record.get("id").asInstanceOf[String], stmt, record) }

      val f = reducer.ask(AskReducedResult)(2.seconds)
      val Success(result) = f.value.get
      result should be(Array(List(5.0, 7.0, 35.0, 9.0, 0.0), List(5.0, 7.0, 35.0, 9.0, 0.0), List(5.0, 7.0, 35.0, 9.0, 0.0), List(5.0, 7.0, 35.0, 9.0, 0.0), List(5.0, 7.0, 35.0, 9.0, 0.0)))
    }

    "query with groupby" in {
      val q = "SELECT AVG(a.registerTime) FROM account a " +
        "WHERE a.registerTime > 1 GROUP BY a.lastLoginTime ORDER BY a.id"
      val stmt = parse(q)

      val reducer = TestActorRef(JPQLReducer.props("test", stmt))

      records() foreach { record => reducer ! collect(record.get("id").asInstanceOf[String], stmt, record) }

      val f = reducer.ask(AskReducedResult)(2.seconds)
      val Success(result) = f.value.get
      result should be(Array(List(5.5), List(5.0), List(6.0)))
    }

    "query with groupby and having" in {
      val q = "SELECT AVG(a.registerTime) FROM account a " +
        "WHERE a.registerTime > 1 GROUP BY a.lastLoginTime HAVING a.registerTime > 5 ORDER BY a.id"
      val stmt = parse(q)

      val reducer = TestActorRef(JPQLReducer.props("test", stmt))

      records() foreach { record => reducer ! collect(record.get("id").asInstanceOf[String], stmt, record) }

      val f = reducer.ask(AskReducedResult)(2.seconds)
      val Success(result) = f.value.get
      result should be(Array(List(5.5), List(5.0), List(6.0)))
    }
  }
}