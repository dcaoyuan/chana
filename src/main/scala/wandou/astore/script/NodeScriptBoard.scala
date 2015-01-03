package wandou.astore.script

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.Props
import akka.contrib.pattern.DistributedPubSubExtension
import akka.contrib.pattern.DistributedPubSubMediator
import java.util.concurrent.locks.ReentrantReadWriteLock
import javax.script.Compilable
import javax.script.CompiledScript
import javax.script.ScriptEngineManager
import scala.collection.mutable
import scala.util.Failure
import scala.util.Success

/**
 * @param   entity name
 * @param   script id
 * @param   JavaScript code in string
 */
final case class PutScript(entity: String, id: String, script: String)
final case class DelScript(entity: String, id: String)

object NodeScriptBoard {
  /**
   * There is a sbt issue related to classloader, anyway, use new ScriptEngineManager(null),
   * by adding 'null' classloader solves this issue:
   * https://github.com/playframework/playframework/issues/2532
   */
  lazy val engineManager = new ScriptEngineManager(null)
  lazy val engine = engineManager.getEngineByName("nashorn")

  private val entityToScripts = new mutable.HashMap[String, collection.Map[String, CompiledScript]].withDefaultValue(new mutable.HashMap[String, CompiledScript]())
  private val EMPTY_SCRIPTS = Map[String, CompiledScript]()
  private val scriptsLock = new ReentrantReadWriteLock()

  private def putScript(entity: String, id: String, compiledScript: CompiledScript): Unit =
    try {
      scriptsLock.writeLock.lock
      entityToScripts(entity) += (id -> compiledScript)
    } finally {
      scriptsLock.writeLock.unlock
    }

  private def delScript(entity: String, id: String): Unit =
    try {
      scriptsLock.writeLock.lock
      entityToScripts(entity) -= id
    } finally {
      scriptsLock.writeLock.unlock
    }

  def scriptsOf(entity: String) =
    try {
      scriptsLock.readLock.lock
      entityToScripts.getOrElse(entity, EMPTY_SCRIPTS)
    } finally {
      scriptsLock.readLock.unlock
    }

  val SCRIPT_BOARD = "scriptBoard"
  private var singletons: NodeSingletons = _
  private val singletonsMutex = new AnyRef()

  final class NodeSingletons(system: ActorSystem) {
    val scriptBoard = system.actorOf(Props(classOf[NodeScriptBoard]), SCRIPT_BOARD)
  }

  def apply(system: ActorSystem): NodeSingletons = {
    if (singletons eq null) {
      singletonsMutex synchronized {
        if (singletons eq null) {
          singletons = new NodeSingletons(system)
        }
      }
    }
    singletons
  }

}

class NodeScriptBoard extends Actor with ActorLogging {
  log.info("{} started.", self.path)

  val mediator = DistributedPubSubExtension(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

  def receive = {
    case x @ PutScript(entity, id, script) =>
      log.info("Got: {}", x)
      try {
        val compiledScript = NodeScriptBoard.engine.asInstanceOf[Compilable].compile(script)
        NodeScriptBoard.putScript(entity, id, compiledScript)
        sender() ! Success(id)
      } catch {
        case ex: Throwable =>
          log.error(ex, ex.getMessage)
          sender() ! Failure(ex)
      }
    case x @ DelScript(entity, id) =>
      log.info("Got: {}", x)
      try {
        NodeScriptBoard.delScript(entity, id)
        sender() ! Success(id)
      } catch {
        case ex: Throwable =>
          log.error(ex, ex.getMessage)
          sender() ! Failure(ex)
      }
  }
}
