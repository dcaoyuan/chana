package chana.jpql

import akka.actor.ActorSystem
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import chana.jpql.JPQLReducer.AskReducedResult
import chana.jpql.nodes.JPQLParser
import chana.jpql.rats.JPQLGrammar
import chana.schema.SchemaBoard
import com.typesafe.config.ConfigFactory
import java.io.StringReader
import org.apache.avro.generic.GenericData.Record
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import xtc.tree.Node
import scala.concurrent.duration._

class JPQLReducerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
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

      val chargeRecord = chargeRecordBuilder.build()
      chargeRecord.put("time", id * 1000L)
      chargeRecord.put("amount", id * 100.0)
      record.put("lastChargeRecord", chargeRecord)

      (id.toString, record)
    }
  }

  def parse(query: String) = {
    val reader = new StringReader(query)
    val grammar = new JPQLGrammar(reader, "")
    val r = grammar.pJPQL(0)
    val rootNode = r.semanticValue[Node]
    info("\n\n## " + query + " ##")

    // now let's do JPQLParsing
    val parser = new JPQLParser()
    val stmt = parser.parse(query)
    //info("\nParsed:\n" + stmt)
    val metaEval = new JPQLMetaEvaluator("jpqlKey", schemaBoard)
    val meta = metaEval.collectMeta(stmt, null)
    info("meta:\n" + meta)
    meta.asInstanceOf[JPQLSelect]
  }

  def gatherProjection(id: String, meta: JPQLSelect, record: Record) = {
    val e = new JPQLMapperSelect(id, meta)
    val projection = e.gatherProjection(record)
    projection match {
      case x: BinaryProjection => info("\nCollected: " + id + ", " + RecordProjection(chana.avro.avroDecode[Record](x.projection, meta.projectionSchema.head).get))
      case x: RemoveProjection => info("\nCollected: " + x)
    }
    projection
  }

  "JPQLReduceEvaluator" must {

    "query fields" in {

      val q = "SELECT a.registerTime FROM account a " +
        "WHERE a.registerTime >= 5 ORDER BY a.registerTime"

      val meta = parse(q)
      val reducer = system.actorOf(JPQLReducer.props("test", meta))

      records() foreach { case (id, record) => reducer ! gatherProjection(id, meta, record) }

      reducer ! AskReducedResult
      expectMsgPF(2.seconds) {
        case result: Array[List[_]] => result should be(Array(List(5), List(6), List(7), List(8), List(9)))
      }
    }

    "query deep fields" in {

      val q = "SELECT a.registerTime, a.lastChargeRecord.time FROM account a " +
        "WHERE a.registerTime >= 5 ORDER BY a.registerTime"

      val meta = parse(q)
      val reducer = system.actorOf(JPQLReducer.props("test", meta))

      records() foreach { case (id, record) => reducer ! gatherProjection(id, meta, record) }

      reducer ! AskReducedResult
      expectMsgPF(2.seconds) {
        case result: Array[List[_]] => result should be(Array(List(5, 5000), List(6, 6000), List(7, 7000), List(8, 8000), List(9, 9000)))
      }
    }

    "query fields order desc" in {

      val q = "SELECT a.registerTime FROM account a " +
        "WHERE a.registerTime >= 5 ORDER BY a.registerTime DESC"

      val meta = parse(q)
      val reducer = system.actorOf(JPQLReducer.props("test", meta))

      records() foreach { case (id, record) => reducer ! gatherProjection(id, meta, record) }

      reducer ! AskReducedResult
      expectMsgPF(2.seconds) {
        case result: Array[List[_]] => result should be(Array(List(9), List(8), List(7), List(6), List(5)))
      }
    }

    "query fields order by string desc" in {

      val q = "SELECT a.id, a.registerTime FROM account a " +
        "WHERE a.registerTime >= 5 ORDER BY a.id DESC"

      val meta = parse(q)
      val reducer = system.actorOf(JPQLReducer.props("test", meta))

      records() foreach { case (id, record) => reducer ! gatherProjection(id, meta, record) }

      reducer ! AskReducedResult
      expectMsgPF(2.seconds) {
        case result: Array[List[_]] => result should be(Array(List("9", 9), List("8", 8), List("7", 7), List("6", 6), List("5", 5)))
      }
    }

    "query aggregate functions" in {

      val q = "SELECT COUNT(a.id), AVG(a.registerTime), SUM(a.registerTime), MAX(a.registerTime), MIN(a.registerTime) FROM account a " +
        "WHERE a.registerTime >= 5 ORDER BY a.id DESC"

      val meta = parse(q)
      val reducer = system.actorOf(JPQLReducer.props("test", meta))

      records() foreach { case (id, record) => reducer ! gatherProjection(id, meta, record) }

      reducer ! AskReducedResult
      expectMsgPF(2.seconds) {
        case result: Array[List[_]] => result should be(Array(
          List(5.0, 7.0, 35.0, 9.0, 0.0)))
      }
    }

    "query with groupby" in {

      val q = "SELECT AVG(a.registerTime) FROM account a " +
        "WHERE a.registerTime > 1 GROUP BY a.lastLoginTime ORDER BY a.id"

      val meta = parse(q)
      val reducer = system.actorOf(JPQLReducer.props("test", meta))

      records() foreach { case (id, record) => reducer ! gatherProjection(id, meta, record) }

      reducer ! AskReducedResult
      expectMsgPF(2.seconds) {
        case result: Array[List[_]] => result should be(Array(List(5.5), List(5.0), List(6.0)))
      }
    }

    "query with groupby and having" in {

      val q = "SELECT AVG(a.registerTime) as average FROM account a " +
        "WHERE a.registerTime > 1 GROUP BY a.lastLoginTime HAVING average >= 5.5 ORDER BY a.id"

      val meta = parse(q)
      val reducer = system.actorOf(JPQLReducer.props("test", meta))

      records() foreach { case (id, record) => reducer ! gatherProjection(id, meta, record) }

      reducer ! AskReducedResult
      expectMsgPF(2.seconds) {
        case result: Array[List[_]] => result should be(Array(List(5.5), List(6.0)))
      }
    }

    "query with join" in {

      val q = "SELECT a.registerTime, a.chargeRecords FROM account a JOIN a.chargeRecords c " +
        "WHERE a.registerTime >= 5 AND c.time > 1 ORDER BY a.registerTime"

      val meta = parse(q)
      val reducer = system.actorOf(JPQLReducer.props("test", meta))

      records() foreach { case (id, record) => reducer ! gatherProjection(id, meta, record) }

      reducer ! AskReducedResult
      expectMsgPF(2.seconds) {
        case result: Array[List[_]] => result map { xs =>
          xs.map {
            case chargeRecord: Record => (chargeRecord.get("time"), chargeRecord.get("amount"))
            case x                    => x
          }
        } should be(Array(List(5, (2, -50.0)), List(6, (2, -50.0)), List(7, (2, -50.0)), List(8, (2, -50.0)), List(9, (2, -50.0))))
      }
    }

    "query with join and INDEX fucntion" in {

      val q = "SELECT a.registerTime, a.chargeRecords FROM account a JOIN a.chargeRecords c " +
        "WHERE a.registerTime >= 5 AND c.time > 0 AND INDEX(c) = 2 ORDER BY a.registerTime"

      val meta = parse(q)
      val reducer = system.actorOf(JPQLReducer.props("test", meta))

      records() foreach { case (id, record) => reducer ! gatherProjection(id, meta, record) }

      reducer ! AskReducedResult
      expectMsgPF(2.seconds) {
        case result: Array[List[_]] => result map { xs =>
          xs.map {
            case chargeRecord: Record => (chargeRecord.get("time"), chargeRecord.get("amount"))
            case x                    => x
          }
        } should be(Array(List(5, (2, -50.0)), List(6, (2, -50.0)), List(7, (2, -50.0)), List(8, (2, -50.0)), List(9, (2, -50.0))))
      }
    }

    "query with join and KEY/VALUE function" in {

      val q = "SELECT a.registerTime, d, CONCAT('k=', KEY(d)), VALUE(d).numBlackApps FROM account a JOIN a.devApps d " +
        "WHERE a.registerTime >= 5 AND KEY(d) = 'b' ORDER BY a.registerTime"

      val meta = parse(q)
      val reducer = system.actorOf(JPQLReducer.props("test", meta))

      records() foreach { case (id, record) => reducer ! gatherProjection(id, meta, record) }

      reducer ! AskReducedResult
      expectMsgPF(2.seconds) {
        case result: Array[List[_]] => result map { xs =>
          xs.map {
            case devApp: java.util.Map.Entry[CharSequence, Record] @unchecked => (devApp.getKey.toString, devApp.getValue.get("numBlackApps"), devApp.getValue.get("numInstalledApps"))
            case devAppValue: Record => (devAppValue.get("numBlackApps"), devAppValue.get("numInstalledApps"))
            case x => x
          }
        } should be(Array(
          List(5, ("b", 2, 0), "k=b", 2),
          List(6, ("b", 2, 0), "k=b", 2),
          List(7, ("b", 2, 0), "k=b", 2),
          List(8, ("b", 2, 0), "k=b", 2),
          List(9, ("b", 2, 0), "k=b", 2)))
      }
    }

    "query with join and KEY/VALUE function, without refer to d directly" in {

      val q = "SELECT a.registerTime, CONCAT('k=', KEY(d)), VALUE(d).numBlackApps FROM account a JOIN a.devApps d " +
        "WHERE a.registerTime >= 5 AND KEY(d) = 'b' ORDER BY a.registerTime"

      val meta = parse(q)
      val reducer = system.actorOf(JPQLReducer.props("test", meta))

      records() foreach { case (id, record) => reducer ! gatherProjection(id, meta, record) }

      reducer ! AskReducedResult
      expectMsgPF(2.seconds) {
        case result: Array[List[_]] => result map { xs =>
          xs.map {
            case devApp: java.util.Map.Entry[CharSequence, Record] @unchecked => (devApp.getKey.toString, devApp.getValue.get("numBlackApps"), devApp.getValue.get("numInstalledApps"))
            case devAppValue: Record => (devAppValue.get("numBlackApps"), devAppValue.get("numInstalledApps"))
            case x => x
          }
        } should be(Array(
          List(5, "k=b", 2),
          List(6, "k=b", 2),
          List(7, "k=b", 2),
          List(8, "k=b", 2),
          List(9, "k=b", 2)))
      }
    }
  }
}