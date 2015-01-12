package wandou.astore.script

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.actor.Props
import akka.pattern.ask
import akka.cluster.Cluster
import java.util.concurrent.locks.ReentrantReadWriteLock
import javax.script.Compilable
import javax.script.CompiledScript
import javax.script.ScriptEngineManager
import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import wandou.astore.DistributedStatusBoard
import wandou.astore.DistributedStatusBoard.Put
import wandou.astore.DistributedStatusBoard.PutAck
import wandou.astore.DistributedStatusBoard.Remove
import wandou.astore.DistributedStatusBoard.RemoveAck

/**
 * @param   entity name
 * @param   script id
 * @param   JavaScript code in string
 */
final case class PutScript(entity: String, field: String, id: String, script: String)
final case class RemoveScript(entity: String, field: String, id: String)

/**
 * Extension that starts a [[DistributedScriptBoard]] actor
 * with settings defined in config section `wandou.astore.script-board`.
 */
object DistributedScriptBoard extends ExtensionId[DistributedScriptBoardExtension] with ExtensionIdProvider {
  // -- implementation of akka extention 
  override def get(system: ActorSystem) = super.get(system)
  override def lookup = DistributedScriptBoard
  override def createExtension(system: ExtendedActorSystem) = new DistributedScriptBoardExtension(system)
  // -- end of implementation of akka extention 

  /**
   * Scala API: Factory method for `DistributedStatusBoard` [[akka.actor.Props]].
   */
  def props(
    role: Option[String],
    gossipInterval: FiniteDuration = 1.second,
    removedTimeToLive: FiniteDuration = 2.minutes,
    maxDeltaElements: Int = 3000): Props = Props(classOf[DistributedScriptBoard], role, gossipInterval, removedTimeToLive, maxDeltaElements)

  /**
   * Java API: Factory method for `DistributedScriptBoard` [[akka.actor.Props]].
   */
  def props(
    role: String,
    gossipInterval: FiniteDuration,
    removedTimeToLive: FiniteDuration,
    maxDeltaElements: Int): Props = props(roleOption(role), gossipInterval, removedTimeToLive, maxDeltaElements)

  /**
   * Java API: Factory method for `DistributedScriptBoard` [[akka.actor.Props]]
   * with default values.
   */
  def defaultProps(role: String): Props = props(roleOption(role))

  def roleOption(role: String): Option[String] = role match {
    case null | "" => None
    case _         => Some(role)
  }

  /**
   * There is a sbt issue related to classloader, anyway, use new ScriptEngineManager(null),
   * by adding 'null' classloader solves this issue:
   * https://github.com/playframework/playframework/issues/2532
   */
  lazy val engineManager = new ScriptEngineManager(null)
  lazy val engine = engineManager.getEngineByName("nashorn").asInstanceOf[Compilable]

  private val keyToScript = new mutable.HashMap[String, CompiledScript]()
  private val entityFieldToScripts = new mutable.HashMap[String, List[(String, CompiledScript)]].withDefaultValue(Nil)
  private val scriptsLock = new ReentrantReadWriteLock()
  private def keyOf(entity: String, field: String, id: String) = entity + "/" + "field" + "/" + id

  private def putScript(key: String, compiledScript: CompiledScript): Unit = key.split('/') match {
    case Array(entity, field, id) => putScript(entity, field, id, compiledScript)
    case _                        =>
  }
  private def putScript(entity: String, field: String, id: String, compiledScript: CompiledScript): Unit = {
    val entityField = entity + "/" + field
    val key = entityField + "/" + id
    try {
      scriptsLock.writeLock.lock
      keyToScript(key) = compiledScript
      entityFieldToScripts(entityField) = (id, compiledScript) :: entityFieldToScripts(entityField)
    } finally {
      scriptsLock.writeLock.unlock
    }
  }

  private def removeScript(key: String): Unit = key.split('/') match {
    case Array(entity, field, id) => removeScript(entity, field, id)
    case _                        =>
  }
  private def removeScript(entity: String, field: String, id: String): Unit = {
    val entityField = entity + "/" + field
    val key = entityField + "/" + id
    try {
      scriptsLock.writeLock.lock
      keyToScript -= key
      entityFieldToScripts(entityField) = entityFieldToScripts(entityField).filterNot(_._1 == id)
    } finally {
      scriptsLock.writeLock.unlock
    }
  }

  def scriptsOf(entity: String, field: String): List[(String, CompiledScript)] =
    try {
      scriptsLock.readLock.lock
      entityFieldToScripts.getOrElse(entity + "/" + field, Nil)
    } finally {
      scriptsLock.readLock.unlock
    }

}

class DistributedScriptBoard(
    val role: Option[String],
    val gossipInterval: FiniteDuration,
    val removedTimeToLive: FiniteDuration,
    val maxDeltaElements: Int) extends DistributedStatusBoard[String] {

  import context.dispatcher

  def receive = businessReceive orElse generalReceive

  def businessReceive: Receive = {
    case PutScript(entity, field, id, script) =>
      val commander = sender()
      self.ask(Put(entity + "/" + field + "/" + id, script))(5.seconds).mapTo[PutAck].onComplete {
        case Success(ack)  => commander ! Success(id)
        case x: Failure[_] => commander ! x
      }

    case RemoveScript(entity, field, id) =>
      val commander = sender()
      self.ask(Remove(entity + "/" + field + "/" + id))(5.seconds).mapTo[RemoveAck].onComplete {
        case Success(ack)  => commander ! Success(id)
        case x: Failure[_] => commander ! x
      }
  }

  override def onPut(keys: Set[String]) {
    for {
      (owner, bucket) <- registry
      (key, valueHolder) <- bucket.content if keys.contains(key)
      script <- valueHolder.value
    } {
      log.info("put script [{}]:\n{} ", key, script)
      try {
        val compiledScript = DistributedScriptBoard.engine.compile(script)
        DistributedScriptBoard.putScript(key, compiledScript)
      } catch {
        case ex: Throwable => log.error(ex, ex.getMessage)
      }
    }
  }

  override def onRemoved(keys: Set[String]) {
    log.info("remove scripts: {}", keys)
    keys foreach DistributedScriptBoard.removeScript
  }

}

class DistributedScriptBoardExtension(system: ExtendedActorSystem) extends Extension {

  private val config = system.settings.config.getConfig("wandou.astore.script-board")
  private val role: Option[String] = config.getString("role") match {
    case "" => None
    case r  => Some(r)
  }

  /**
   * Returns true if this member is not tagged with the role configured for the
   * mediator.
   */
  def isTerminated: Boolean = Cluster(system).isTerminated || !role.forall(Cluster(system).selfRoles.contains)

  /**
   * The [[DistributedScriptBoard]]
   */
  val board: ActorRef = {
    if (isTerminated)
      system.deadLetters
    else {
      val gossipInterval = config.getDuration("gossip-interval", MILLISECONDS).millis
      val removedTimeToLive = config.getDuration("removed-time-to-live", MILLISECONDS).millis
      val maxDeltaElements = config.getInt("max-delta-elements")
      val name = config.getString("name")
      system.actorOf(
        DistributedScriptBoard.props(role, gossipInterval, removedTimeToLive, maxDeltaElements),
        name)
    }
  }
}

