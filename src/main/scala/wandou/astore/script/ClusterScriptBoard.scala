package wandou.astore.script

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
 * Cluster singleton script board
 */
object ClusterScriptBoard {
  def props() = Props(classOf[ClusterScriptBoard])

  val scriptBoardName = "clusterScriptBoard"
  val scriptBoardManagerName = scriptBoardName + "Manager"
  val scriptBoardPath = "/user/" + scriptBoardManagerName + "/" + scriptBoardName
  val scriptBoardProxyName = scriptBoardName + "Proxy"
  val scriptBoardProxyPath = "/user/" + scriptBoardProxyName

  /**
   *  The path of singleton will be "/user/managerName/singletonName"
   */
  def startClusterScriptBoard(system: ActorSystem, role: Option[String]) = {
    val managerName = scriptBoardManagerName
    system.actorOf(
      ClusterSingletonManager.props(
        singletonProps = props(),
        singletonName = scriptBoardName,
        terminationMessage = PoisonPill,
        role = role),
      name = managerName)
  }

  def startClusterScriptBoardProxy(system: ActorSystem, role: Option[String]) {
    val proxy = system.actorOf(
      ClusterSingletonProxy.props(
        singletonPath = scriptBoardPath,
        role = role),
      name = scriptBoardProxyName)
    ClusterReceptionistExtension(system).registerService(proxy)
  }

  def clusterScriptBoardProxy(system: ActorSystem) = system.actorSelection(scriptBoardProxyPath)
}

class ClusterScriptBoard extends Actor with ActorLogging {
  import context.dispatcher

  val mediator = DistributedPubSubExtension(context.system).mediator
  val timeout: Timeout = 20.seconds
  val allScriptBoardsPath = "/user/" + NodeScriptBoard.SCRIPT_BOARD

  def receive = {
    case x: PutScript =>
      mediator ! DistributedPubSubMediator.SendToAll(allScriptBoardsPath, x)
      sender ! Success(x.entity)
    case x: DelScript =>
      mediator ! DistributedPubSubMediator.SendToAll(allScriptBoardsPath, x)
      sender ! Success(x.entity)
  }
}