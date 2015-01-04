package wandou.astore.script

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.Stash
import akka.contrib.pattern.ClusterReceptionistExtension
import akka.contrib.pattern.ClusterSingletonManager
import akka.contrib.pattern.ClusterSingletonProxy
import akka.contrib.pattern.DistributedPubSubExtension
import akka.contrib.pattern.DistributedPubSubMediator
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

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

  private object Internal {
    case class Done(result: Try[String])
  }
}

class ClusterScriptBoard extends Actor with Stash with ActorLogging {
  import ClusterScriptBoard.Internal._
  import context.dispatcher

  val mediator = DistributedPubSubExtension(context.system).mediator
  val timeout: Timeout = 5.seconds
  val allScriptBoardsPath = "/user/" + NodeScriptBoard.SCRIPT_BOARD

  def receive = ready

  def ready: Receive = {
    case x: PutScript =>
      mediator ! DistributedPubSubMediator.SendToAll(allScriptBoardsPath, x)
      // TODO wait for all scriptBoards return Success ?
      context.system.scheduler.scheduleOnce(2.seconds, self, Done(Success(x.entity)))
      context.become(waitForComplete(sender()))
    case x: DelScript =>
      mediator ! DistributedPubSubMediator.SendToAll(allScriptBoardsPath, x)
      // TODO wait for all scriptBoards return Success ?
      context.system.scheduler.scheduleOnce(2.seconds, self, Done(Success(x.entity)))
      context.become(waitForComplete(sender()))
  }

  def waitForComplete(commander: ActorRef): Receive = {
    case Done(x) =>
      x match {
        case x @ Success(_) => commander ! x
        case x @ Failure(_) => commander ! x
      }
      context.become(ready)
      unstashAll()

    case x =>
      stash()
  }
}