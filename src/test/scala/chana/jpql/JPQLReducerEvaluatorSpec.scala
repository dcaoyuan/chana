package chana.jpql

import akka.actor.ActorSystem
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import chana.jpql.JPQLReducer.AskReducedResult
import chana.jpql.nodes.JPQLParser
import chana.jpql.nodes.Statement
import chana.jpql.rats.JPQLGrammar
import chana.schema.SchemaBoard
import com.typesafe.config.ConfigFactory
import java.io.StringReader
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import xtc.tree.Node
import scala.concurrent.duration._

class JPQLReducerEvaluatorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  def this() = this(ActorSystem("ChanaSystem", ConfigFactory.parseString("""
  akka.actor {
      provider = "akka.cluster.ClusterActorRefProvider"                                                                      
  }
  akka.remote.netty.tcp.hostname = "localhost"
  # set port to random to by pass the ports that will be occupied by ChanaClusterSpec test
  akka.remote.netty.tcp.port = 0
  """)))

  import chana.avro.AvroRecords._

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  val schemaBoard = new SchemaBoard {
    val entityToSchema = Map("account" -> schema)
    def schemaOf(entityName: String) = entityToSchema.get(entityName)
  }

  def records() = {
    for (id <- 0 to 9) yield {
      val record = initAccount()
      record.put("registerTime", id.toLong)
      record.put("lastLoginTime", (id % 3).toLong)
      record.put("id", id.toString)

      val chargeRecord = chargeRecordBuilder.build()
      chargeRecord.put("time", id * 1000L)
      chargeRecord.put("amount", id * 100.0)
      record.put("lastChargeRecord", chargeRecord)

      record
    }
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
    val metaEval = new JPQLMetaEvaluator("2", schemaBoard)
    val projectionSchema = metaEval.collectMetadata(stmt, null).head
    info("Projection Schema:\n" + projectionSchema)
    (stmt, projectionSchema)
  }

  def gatherProjection(entityId: String, stmt: Statement, projectionSchema: Schema, record: Record) = {
    val e = new JPQLMapperEvaluator(record.getSchema, projectionSchema)
    val projection = e.gatherProjection(entityId, stmt, record)
    projection match {
      case x: BinaryProjection => info("\nCollected: " + x.id + ", " + RecordProjection(chana.avro.avroDecode[Record](x.projection, projectionSchema).get))
      case x: RemoveProjection => info("\nCollected: " + x)
    }
    projection
  }

  "JPQLReduceEvaluator" must {

    "query fields" in {
      val q = "SELECT a.registerTime FROM account a " +
        "WHERE a.registerTime >= 5 ORDER BY a.registerTime"
      val (stmt, projectionSchema) = parse(q)

      val reducer = system.actorOf(JPQLReducer.props("test", stmt, projectionSchema))

      records() foreach { record => reducer ! gatherProjection(record.get("id").asInstanceOf[String], stmt, projectionSchema, record) }

      reducer ! AskReducedResult
      expectMsgPF(2.seconds) {
        case result: Array[List[_]] => result should be(Array(List(5), List(6), List(7), List(8), List(9)))
      }
    }

    "query deep fields" in {
      val q = "SELECT a.registerTime, a.lastChargeRecord.time FROM account a " +
        "WHERE a.registerTime >= 5 ORDER BY a.registerTime"
      val (stmt, projectionSchema) = parse(q)

      val reducer = system.actorOf(JPQLReducer.props("test", stmt, projectionSchema))

      records() foreach { record => reducer ! gatherProjection(record.get("id").asInstanceOf[String], stmt, projectionSchema, record) }

      reducer ! AskReducedResult
      expectMsgPF(2.seconds) {
        case result: Array[List[_]] => result should be(Array(List(5, 5000), List(6, 6000), List(7, 7000), List(8, 8000), List(9, 9000)))
      }
    }

    "query fields order desc" in {
      val q = "SELECT a.registerTime FROM account a " +
        "WHERE a.registerTime >= 5 ORDER BY a.registerTime DESC"
      val (stmt, projectionSchema) = parse(q)

      val reducer = system.actorOf(JPQLReducer.props("test", stmt, projectionSchema))

      records() foreach { record => reducer ! gatherProjection(record.get("id").asInstanceOf[String], stmt, projectionSchema, record) }

      reducer ! AskReducedResult
      expectMsgPF(2.seconds) {
        case result: Array[List[_]] => result should be(Array(List(9), List(8), List(7), List(6), List(5)))
      }
    }

    "query fields order by string desc" in {
      val q = "SELECT a.id, a.registerTime FROM account a " +
        "WHERE a.registerTime >= 5 ORDER BY a.id DESC"
      val (stmt, projectionSchema) = parse(q)

      val reducer = system.actorOf(JPQLReducer.props("test", stmt, projectionSchema))

      records() foreach { record => reducer ! gatherProjection(record.get("id").asInstanceOf[String], stmt, projectionSchema, record) }

      reducer ! AskReducedResult
      expectMsgPF(2.seconds) {
        case result: Array[List[_]] => result should be(Array(List("9", 9), List("8", 8), List("7", 7), List("6", 6), List("5", 5)))
      }
    }

    "query aggregate functions" in {
      val q = "SELECT COUNT(a.id), AVG(a.registerTime), SUM(a.registerTime), MAX(a.registerTime), MIN(a.registerTime) FROM account a " +
        "WHERE a.registerTime >= 5 ORDER BY a.id DESC"
      val (stmt, projectionSchema) = parse(q)

      val reducer = system.actorOf(JPQLReducer.props("test", stmt, projectionSchema))

      records() foreach { record => reducer ! gatherProjection(record.get("id").asInstanceOf[String], stmt, projectionSchema, record) }

      reducer ! AskReducedResult
      expectMsgPF(2.seconds) {
        case result: Array[List[_]] => result should be(Array(List(5.0, 7.0, 35.0, 9.0, 0.0), List(5.0, 7.0, 35.0, 9.0, 0.0), List(5.0, 7.0, 35.0, 9.0, 0.0), List(5.0, 7.0, 35.0, 9.0, 0.0), List(5.0, 7.0, 35.0, 9.0, 0.0)))
      }
    }

    "query with groupby" in {
      val q = "SELECT AVG(a.registerTime) FROM account a " +
        "WHERE a.registerTime > 1 GROUP BY a.lastLoginTime ORDER BY a.id"
      val (stmt, projectionSchema) = parse(q)

      val reducer = system.actorOf(JPQLReducer.props("test", stmt, projectionSchema))

      records() foreach { record => reducer ! gatherProjection(record.get("id").asInstanceOf[String], stmt, projectionSchema, record) }

      reducer ! AskReducedResult
      expectMsgPF(2.seconds) {
        case result: Array[List[_]] => result should be(Array(List(5.5), List(5.0), List(6.0)))
      }
    }

    "query with groupby and having" in {
      val q = "SELECT AVG(a.registerTime) FROM account a " +
        "WHERE a.registerTime > 1 GROUP BY a.lastLoginTime HAVING a.registerTime > 5 ORDER BY a.id"
      val (stmt, projectionSchema) = parse(q)

      val reducer = system.actorOf(JPQLReducer.props("test", stmt, projectionSchema))

      records() foreach { record => reducer ! gatherProjection(record.get("id").asInstanceOf[String], stmt, projectionSchema, record) }

      reducer ! AskReducedResult
      expectMsgPF(2.seconds) {
        case result: Array[List[_]] => result should be(Array(List(5.5), List(5.0), List(6.0)))
      }
    }

    "query with join" in {
      val q = "SELECT a.registerTime, a.chargeRecords FROM account a JOIN a.chargeRecords c " +
        "WHERE a.registerTime >= 5 AND c.time > 1 ORDER BY a.registerTime"
      val (stmt, projectionSchema) = parse(q)

      val reducer = system.actorOf(JPQLReducer.props("test", stmt, projectionSchema))

      records() foreach { record => reducer ! gatherProjection(record.get("id").asInstanceOf[String], stmt, projectionSchema, record) }

      reducer ! AskReducedResult

      import scala.collection.JavaConversions._
      expectMsgPF(2.seconds) {
        case result: Array[List[_]] => result map { xs =>
          xs.map {
            case chargeRecords: java.util.Collection[Record] @unchecked => chargeRecords map { x => (x.get("time"), x.get("amount")) }
            case x => x
          }
        } should be(Array(List(5, List((2, -50.0))), List(6, List((2, -50.0))), List(7, List((2, -50.0))), List(8, List((2, -50.0))), List(9, List((2, -50.0)))))
      }
    }

    "query with join and INDEX fucntion" in {
      val q = "SELECT a.registerTime, a.chargeRecords FROM account a JOIN a.chargeRecords c " +
        "WHERE a.registerTime >= 5 AND c.time > 0 AND INDEX(c) = 2 ORDER BY a.registerTime"
      val (stmt, projectionSchema) = parse(q)

      val reducer = system.actorOf(JPQLReducer.props("test", stmt, projectionSchema))

      records() foreach { record => reducer ! gatherProjection(record.get("id").asInstanceOf[String], stmt, projectionSchema, record) }

      reducer ! AskReducedResult

      import scala.collection.JavaConversions._
      expectMsgPF(2.seconds) {
        case result: Array[List[_]] => result map { xs =>
          xs.map {
            case chargeRecords: java.util.Collection[Record] @unchecked =>
              chargeRecords map { x => (x.get("time"), x.get("amount")) }
            case x => x
          }
        } should be(Array(List(5, List((2, -50.0))), List(6, List((2, -50.0))), List(7, List((2, -50.0))), List(8, List((2, -50.0))), List(9, List((2, -50.0)))))
      }
    }

    "query with join and KEY/VALUE fucntion" in {
      val q = "SELECT a.registerTime, a.devApps FROM account a JOIN a.devApps c " +
        "WHERE a.registerTime >= 5 AND KEY(c) = 'b' ORDER BY a.registerTime"
      val (stmt, projectionSchema) = parse(q)

      val reducer = system.actorOf(JPQLReducer.props("test", stmt, projectionSchema))

      records() foreach { record => reducer ! gatherProjection(record.get("id").asInstanceOf[String], stmt, projectionSchema, record) }

      reducer ! AskReducedResult

      import scala.collection.JavaConversions._
      expectMsgPF(2.seconds) {
        case result: Array[List[_]] => result map { xs =>
          xs.map {
            case devApps: java.util.Map[CharSequence, Record] @unchecked =>
              devApps.map { case (key, value) => (key.toString, value.get("numBlackApps"), value.get("numInstalledApps")) }.toList
            case x => x
          }
        } should be(Array(List(5, List(("b", 2, 0))), List(6, List(("b", 2, 0))), List(7, List(("b", 2, 0))), List(8, List(("b", 2, 0))), List(9, List(("b", 2, 0)))))
      }
    }
  }
}