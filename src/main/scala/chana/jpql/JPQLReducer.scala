package chana.jpql

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.Stash
import akka.contrib.pattern.{ ClusterReceptionistExtension, ClusterSingletonManager, ClusterSingletonProxy }
import chana.jpql.nodes.Statement
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import scala.concurrent.duration._
import scala.util.Sorting

object DataSetOrdering extends Ordering[(String, DataSet)] {
  def compare(x: (String, DataSet), y: (String, DataSet)) = {
    var xs = x._2.orderby
    var ys = y._2.orderby
    if (xs.nonEmpty) {
      (xs.head, ys.head) match {
        case (a: Number, b: Number)               => a.doubleValue.compareTo(b.doubleValue)
        case (a: String, b: String)               => a.compareToIgnoreCase(b)
        case (a: LocalTime, b: LocalTime)         => if (a.isBefore(b)) -1 else if (a.isAfter(b)) 1 else 0
        case (a: LocalDate, b: LocalDate)         => if (a.isBefore(b)) -1 else if (a.isAfter(b)) 1 else 0
        case (a: LocalDateTime, b: LocalDateTime) => if (a.isBefore(b)) -1 else if (a.isAfter(b)) 1 else 0
      }
    } else 0
  }
}

object JPQLReducer {
  def props(jpqlKey: String, stmt: Statement): Props = Props(classOf[JPQLReducer], jpqlKey, stmt)

  /**
   * @param entityId  id of reporting entity
   * @param values    values that are needed to reduce. It's deleted when null
   */
  final case class SelectToReducer(entityId: String, dataset: DataSet)
  case object AskResult
  case object AskReducedResult

  val role = Some("jpql")

  def singletonManagerName(key: String) = "jpqlSingleton-" + key
  def reducerPath(key: String) = "/user/" + singletonManagerName(key) + "/" + key
  def reducerProxyName(key: String) = "jpqlProxy-" + key
  def reducerProxyPath(key: String) = "/user/" + reducerProxyName(key)

  def startReducer(system: ActorSystem, role: Option[String], jpqlKey: String, stmt: Statement) = {
    system.actorOf(
      ClusterSingletonManager.props(
        singletonProps = props(jpqlKey, stmt),
        singletonName = jpqlKey,
        terminationMessage = PoisonPill,
        role = role),
      name = singletonManagerName(jpqlKey))
  }

  def startReducerProxy(system: ActorSystem, role: Option[String], jpqlKey: String) {
    val proxy = system.actorOf(
      ClusterSingletonProxy.props(singletonPath = reducerPath(jpqlKey), role = role),
      reducerProxyName(jpqlKey))
    ClusterReceptionistExtension(system).registerService(proxy)
  }

  def reducerProxy(system: ActorSystem, jpqlKey: String) = system.actorSelection(reducerProxyPath(jpqlKey))
}

class JPQLReducer(jqplKey: String, statement: Statement) extends Actor with Stash with ActorLogging {

  import chana.jpql.JPQLReducer._
  import context.dispatcher

  log.info("Aggregator {} started", jqplKey)
  ClusterReceptionistExtension(context.system).registerService(self)

  private var idToDataSet = Map[String, DataSet]()
  private var isResultUpdated = false
  private var prevUpdateTime: LocalDate = _
  private var today: LocalDate = _
  private val evaluator = new JPQLReduceEvaluator(log)

  override def preStart {
    prevUpdateTime = LocalDate.now()
  }

  override def postStop() {
    super.postStop()
  }

  def receive: Receive = {
    case SelectToReducer(entityId, res) =>
      isResultUpdated = true
      if (res eq null) {
        idToDataSet -= entityId // remove
      } else {
        idToDataSet += (entityId -> res)
      }

    case AskResult =>
      val commander = sender()
      commander ! idToDataSet.toString

    case AskReducedResult =>
      val commander = sender()
      commander ! reduceValues().toString

    case _ =>
  }

  def reduceValues() /*: Array[List[_]]*/ = {
    val shouldSort = idToDataSet.headOption.fold(false) { _._2.orderby.nonEmpty }
    val dataset = idToDataSet.toArray
    if (shouldSort) {
      Sorting.quickSort(dataset)(DataSetOrdering)
      log.debug("sorted by {} etc", idToDataSet.head._2.orderby)
    }

    evaluator.reset(idToDataSet)
    val len = dataset.length
    val reduced = Array.ofDim[List[Any]](len)
    var i = 0
    while (i < len) {
      val entry = dataset(i)
      reduced(i) = evaluator.visit(statement, entry._2.values)
      i += 1
    }
    log.debug("reduced: {}", reduced.mkString("Array(", ",", ")"))
    reduced.mkString("Array(", ",", ")")
  }
}