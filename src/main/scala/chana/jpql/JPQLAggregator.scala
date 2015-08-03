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
import scala.concurrent.duration._

object JPQLAggregator {
  def props(jpqlKey: String, stmt: Statement): Props = Props(classOf[JPQLAggregator], jpqlKey, stmt)

  /**
   * @param entityId  id of reporting entity
   * @param result    list of selected terms. It's deleted when null
   */
  final case class SelectToAggregator(entityId: String, result: List[Any])
  case object AskResult
  case object AskMergedResult

  val role = Some("jpql")

  def singletonManagerName(key: String) = "jpqlSingleton-" + key
  def aggregatorPath(key: String) = "/user/" + singletonManagerName(key) + "/" + key
  def aggregatorProxyName(key: String) = "jpqlProxy-" + key
  def aggregatorProxyPath(key: String) = "/user/" + aggregatorProxyName(key)

  def startAggregator(system: ActorSystem, role: Option[String], jpqlKey: String, stmt: Statement) = {
    system.actorOf(
      ClusterSingletonManager.props(
        singletonProps = props(jpqlKey, stmt),
        singletonName = jpqlKey,
        terminationMessage = PoisonPill,
        role = role),
      name = singletonManagerName(jpqlKey))
  }

  def startAggregatorProxy(system: ActorSystem, role: Option[String], jpqlKey: String) {
    val proxy = system.actorOf(
      ClusterSingletonProxy.props(singletonPath = aggregatorPath(jpqlKey), role = role),
      aggregatorProxyName(jpqlKey))
    ClusterReceptionistExtension(system).registerService(proxy)
  }

  def aggregatorProxy(system: ActorSystem, jpqlKey: String) = system.actorSelection(aggregatorProxyPath(jpqlKey))
}

class JPQLAggregator(jqplKey: String, statement: Statement) extends Actor with Stash with ActorLogging {

  import chana.jpql.JPQLAggregator._
  import context.dispatcher

  log.info("Aggregator {} started", jqplKey)
  ClusterReceptionistExtension(context.system).registerService(self)

  private var result = Map[String, List[Any]]()
  private var isResultUpdated = false
  private var prevUpdateTime: LocalDate = _
  private var today: LocalDate = _

  val reportingTask = Some(context.system.scheduler.schedule(0 seconds, 5 seconds, self, AskResult))

  override def preStart {
    prevUpdateTime = LocalDate.now()
  }

  override def postStop() {
    super.postStop()
    reportingTask foreach { _.cancel }
  }

  def receive: Receive = {
    case SelectToAggregator(entityId, res) =>
      isResultUpdated = true
      if (res eq null) {
        result = result - entityId // deleted
      } else {
        result = result + (entityId -> res)
      }

    case AskResult =>
      val commander = sender()
      commander ! result.toString

    case AskMergedResult =>
      val commander = sender()
      commander ! mergeResult().toString

    case _ =>
  }

  def mergeResult() = {
    var merged = List[Any]()
    result foreach {
      case (id, xs) =>
        merged = xs ::: merged
    }
    merged
  }
}