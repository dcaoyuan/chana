package wandou.astore.schema

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
 * Cluster singleton schema board
 */
object ClusterSchemaBoard {
  def props() = Props(classOf[ClusterSchemaBoard])

  val schemaBoardName = "clusterSchemaBoard"
  val schemaBoardManagerName = schemaBoardName + "Manager"
  val schemaBoardPath = "/user/" + schemaBoardManagerName + "/" + schemaBoardName
  val schemaBoardProxyName = schemaBoardName + "Proxy"
  val schemaBoardProxyPath = "/user/" + schemaBoardProxyName

  def startClusterSchemaBoard(system: ActorSystem, role: Option[String]) = {
    val managerName = schemaBoardManagerName
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

  private object Internal {
    case class Done(result: Try[String])
  }
}

class ClusterSchemaBoard extends Actor with Stash with ActorLogging {
  import ClusterSchemaBoard.Internal._
  import context.dispatcher

  val mediator = DistributedPubSubExtension(context.system).mediator
  val timeout: Timeout = 5.seconds
  val allSchemaBoardsPath = "/user/" + NodeSchemaBoard.SCHEMA_BOARD

  def receive = ready

  def ready: Receive = {
    case x: PutSchema =>
      mediator ! DistributedPubSubMediator.SendToAll(allSchemaBoardsPath, x)
      // TODO wait for all scriptBoards return Success ?
      context.system.scheduler.scheduleOnce(2.seconds, self, Done(Success(x.entityName)))
      context.become(waitForComplete(sender()))
    case x: DelSchema =>
      mediator ! DistributedPubSubMediator.SendToAll(allSchemaBoardsPath, x)
      // TODO wait for all scriptBoards return Success ?
      context.system.scheduler.scheduleOnce(2.seconds, self, Done(Success(x.entityName)))
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