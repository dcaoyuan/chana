package wandou.astore.schema

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.contrib.pattern.ClusterReceptionistExtension
import akka.contrib.pattern.ClusterSingletonManager
import akka.contrib.pattern.ClusterSingletonProxy
import akka.contrib.pattern.DistributedPubSubExtension
import akka.contrib.pattern.DistributedPubSubMediator
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import scala.util.Success

/**
 * Cluster singleton schema board
 */
object ClusterSchemaBoard {
  def props() = Props(classOf[ClusterSchemaBoard])

  val singletonManageName = "astoreSingletonManager"
  val schemaBoardName = "clusterSchemaBoard"
  val schemaBoardPath = "/user/" + singletonManageName + "/" + schemaBoardName
  val schemaBoardProxyName = schemaBoardName + "Proxy"
  val schemaBoardProxyPath = "/user/" + schemaBoardProxyName

  def startClusterSchemaBoard(system: ActorSystem, role: Option[String]) = {
    val managerName = singletonManageName
    system.actorOf(
      ClusterSingletonManager.props(
        singletonProps = props(),
        singletonName = schemaBoardName,
        terminationMessage = PoisonPill,
        role = role),
      name = managerName)
  }

  def startClusterSchemaBoardProxy(system: ActorSystem, role: Option[String]) {
    val proxy = system.actorOf(
      ClusterSingletonProxy.props(
        singletonPath = schemaBoardPath,
        role = role),
      name = schemaBoardProxyName)
    ClusterReceptionistExtension(system).registerService(proxy)
  }

  def clusterSchemaBoardProxy(system: ActorSystem) = system.actorSelection(schemaBoardProxyPath)
}

class ClusterSchemaBoard extends Actor with ActorLogging {
  import context.dispatcher

  val mediator = DistributedPubSubExtension(context.system).mediator
  val timeout: Timeout = 20 seconds
  val allSchemaBoardsPath = "/user/" + NodeSchemaBoard.SCHEMA_BOARD

  def receive = {
    case x: PutSchema =>
      mediator ! DistributedPubSubMediator.SendToAll(allSchemaBoardsPath, x)
      sender ! Success(x.entityName)
    case x: DelSchema =>
      mediator ! DistributedPubSubMediator.SendToAll(allSchemaBoardsPath, x)
      sender ! Success(x.entityName)
  }
}