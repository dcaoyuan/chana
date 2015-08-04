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

object JPQLReducer {
  def props(jpqlKey: String, stmt: Statement): Props = Props(classOf[JPQLReducer], jpqlKey, stmt)

  /**
   * @param entityId  id of reporting entity
   * @param values    values that are needed to reduce. It's deleted when null
   */
  final case class SelectToReducer(entityId: String, values: Map[String, Any])
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

  private var idToValues = Map[String, Map[String, Any]]()
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
        idToValues -= entityId // deleted
      } else {
        idToValues += (entityId -> res)
      }

    case AskResult =>
      val commander = sender()
      commander ! idToValues.toString

    case AskReducedResult =>
      val commander = sender()
      commander ! reduceValues().toString

    case _ =>
  }

  def reduceValues() = {
    var reduced = List[List[Any]]()
    idToValues foreach {
      case (id, values) if values.nonEmpty =>
        val xs = evaluator.visit(statement, values, idToValues)
        reduced = xs :: reduced
      case _ =>
    }
    log.debug("reduced: {}", reduced)
    reduced
  }
}