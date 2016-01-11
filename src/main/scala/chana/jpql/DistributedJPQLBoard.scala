package chana.jpql

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.ddata.DistributedData
import akka.cluster.ddata.LWWMap
import akka.cluster.ddata.LWWMapKey
import akka.pattern.ask
import chana.avro.ToJson
import chana.jpql.nodes.JPQLParser
import chana.schema.DistributedSchemaBoard
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

/**
 * Extension that starts a [[DistributedJPQLBoard]] actor
 * with settings defined in config section `chana.jpql-board`.
 * Don't forget to enable it in "akka.extensions" of config.
 */
object DistributedJPQLBoard extends ExtensionId[DistributedJPQLBoardExtension] with ExtensionIdProvider {
  // -- implementation of akka extention 
  override def get(system: ActorSystem) = super.get(system)
  override def lookup = DistributedJPQLBoard
  override def createExtension(system: ExtendedActorSystem) = new DistributedJPQLBoardExtension(system)
  // -- end of implementation of akka extention 

  /**
   * Scala API: Factory method for `DistributedJPQLBoard` [[akka.actor.Props]].
   */
  def props(): Props = Props(classOf[DistributedJPQLBoard])

  val keyToJPQL = new ConcurrentHashMap[String, (JPQLMeta, FiniteDuration)]()
  val keyToReducerProxy = new ConcurrentHashMap[String, ActorRef]()
  private val jpqlsLock = new ReentrantReadWriteLock()

  private def putJPQL(system: ActorSystem, key: String, meta: JPQLMeta, interval: FiniteDuration = 1.seconds) {
    keyToJPQL.putIfAbsent(key, (meta, interval)) match {
      case null =>
        JPQLReducer.startReducer(system, JPQLReducer.role, key, meta)
        val proxy = JPQLReducer.startReducerProxy(system, JPQLReducer.role, key)
        keyToReducerProxy.put(key, proxy)
      case old => // TODO if existed
    }
  }

  private def removeJPQL(key: String) {
    keyToJPQL.remove(key)
    keyToReducerProxy.remove(key)
    // TODO remove aggregator
  }

  val DataKey = LWWMapKey[String]("chana-jpqls")
}

class DistributedJPQLBoard extends Actor with ActorLogging {
  import akka.cluster.ddata.Replicator._

  implicit val cluster = Cluster(context.system)
  import context.dispatcher

  val replicator = {
    val x = DistributedData(context.system).replicator
    x ! Subscribe(DistributedJPQLBoard.DataKey, self)
    x
  }

  def receive = {
    case chana.PutJPQL(_, key, jpql, interval) =>
      val commander = sender()

      parseJPQL(key, jpql) match {
        case Success(meta: JPQLSelect) =>
          replicator.ask(Update(DistributedJPQLBoard.DataKey, LWWMap(), WriteAll(60.seconds))(_ + (key -> jpql)))(60.seconds).onComplete {
            case Success(_: UpdateSuccess[_]) =>
              DistributedJPQLBoard.putJPQL(context.system, key, meta, interval)
              log.info("put jpql (Update) [{}]:\n{} ", key, jpql)
              commander ! Success(key)

            case Success(_: UpdateTimeout[_]) => commander ! Failure(chana.UpdateTimeoutException)
            case Success(x: ModifyFailure[_]) => commander ! Failure(x.cause)
            case Success(x)                   => log.warning("Got {}", x)
            case failure: Failure[_]          => commander ! failure
          }

        case Success(meta) =>
          log.warning("{} should be processed by rest actor ", meta)

        case failure @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! failure
      }

    case chana.RemoveJPQL(key) =>
      val commander = sender()

      replicator.ask(Update(DistributedJPQLBoard.DataKey, LWWMap(), WriteAll(60.seconds))(_ - key))(60.seconds).onComplete {
        case Success(_: UpdateSuccess[_]) =>
          log.info("remove jpql (Update): {}", key)
          DistributedJPQLBoard.removeJPQL(key)
          commander ! Success(key)

        case Success(_: UpdateTimeout[_]) => commander ! Failure(chana.UpdateTimeoutException)
        case Success(x: ModifyFailure[_]) => commander ! Failure(x.cause)
        case Success(x)                   => log.warning("Got {}", x)
        case ex: Failure[_]               => commander ! ex
      }

    case chana.AskJPQL(key) =>
      val commander = sender()

      val reducerProxy = DistributedJPQLBoard.keyToReducerProxy.get(key)
      if (reducerProxy ne null) {
        reducerProxy.ask(JPQLReducer.AskReducedResult)(300.seconds).onComplete {
          case Success(result: Array[_]) => commander ! Success(ToJson.toJsonString(result)) // TODO
          case failure                   => commander ! failure
        }
      } else {
        commander ! Failure(new RuntimeException("There is no reducer for " + key))
      }

    // --- commands of akka-data-replication

    case c @ Changed(DistributedJPQLBoard.DataKey) =>
      // check if there were newly added
      val entries = c.get(DistributedJPQLBoard.DataKey).entries
      entries.foreach {
        case (key, jpql) =>
          DistributedJPQLBoard.keyToJPQL.get(key) match {
            case null =>
              parseJPQL(key, jpql) match {
                case Success(meta) =>
                  DistributedJPQLBoard.putJPQL(context.system, key, meta)
                  log.info("put jpql (Changed) [{}]:\n{} ", key, jpql)
                case Failure(ex) =>
                  log.error(ex, ex.getMessage)
              }
            case jpql => // TODO, existed, but changed?
          }
      }

      // check if there were removed
      val toRemove = DistributedJPQLBoard.keyToJPQL.filter(x => !entries.contains(x._1)).keys
      if (toRemove.nonEmpty) {
        log.info("remove jpql (Changed): {}", toRemove)
        toRemove foreach DistributedJPQLBoard.removeJPQL
      }
  }

  private def parseJPQL(jpqlKey: String, jpql: String) = {
    val parser = new JPQLParser()
    try {
      val stmt = parser.parse(jpql)
      val meta = new JPQLMetaEvaluator(jpqlKey, DistributedSchemaBoard).collectMeta(stmt, null)
      Success(meta)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }
}

class DistributedJPQLBoardExtension(system: ExtendedActorSystem) extends Extension {

  private val config = system.settings.config.getConfig("chana.jpql-board")
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
   * The [[DistributedJPQLBoard]]
   */
  val board: ActorRef = {
    if (isTerminated)
      system.deadLetters
    else {
      val name = config.getString("name")
      system.actorOf(
        DistributedJPQLBoard.props(),
        name)
    }
  }
}
