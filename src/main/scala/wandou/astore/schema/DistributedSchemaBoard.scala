package wandou.astore.schema

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
import org.apache.avro.Schema
import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import wandou.astore
import wandou.astore.DistributedStatusBoard
import wandou.astore.DistributedStatusBoard.Put
import wandou.astore.DistributedStatusBoard.PutAck
import wandou.astore.DistributedStatusBoard.Remove
import wandou.astore.DistributedStatusBoard.RemoveAck
import wandou.astore.Entity
import wandou.avro.RecordBuilder

/**
 * Extension that starts a [[DistributedSchemaBoard]] actor
 * with settings defined in config section `wandou.astore.schema-board`.
 */
object DistributedSchemaBoard extends ExtensionId[DistributedSchemaBoardExtension] with ExtensionIdProvider {
  // -- implementation of akka extention 
  override def get(system: ActorSystem) = super.get(system)
  override def lookup = DistributedSchemaBoard
  override def createExtension(system: ExtendedActorSystem) = new DistributedSchemaBoardExtension(system)
  // -- end of implementation of akka extention 

  /**
   * Scala API: Factory method for `DistributedSchemaBoard` [[akka.actor.Props]].
   */
  def props(
    role: Option[String],
    gossipInterval: FiniteDuration = 1.second,
    removedTimeToLive: FiniteDuration = 2.minutes,
    maxDeltaElements: Int = 3000): Props = Props(classOf[DistributedSchemaBoard], role, gossipInterval, removedTimeToLive, maxDeltaElements)

  /**
   * Java API: Factory method for `DistributedSchemaBoard` [[akka.actor.Props]].
   */
  def props(
    role: String,
    gossipInterval: FiniteDuration,
    removedTimeToLive: FiniteDuration,
    maxDeltaElements: Int): Props = props(roleOption(role), gossipInterval, removedTimeToLive, maxDeltaElements)

  /**
   * Java API: Factory method for `DistributedSchemaBoard` [[akka.actor.Props]]
   * with default values.
   */
  def defaultProps(role: String): Props = props(roleOption(role))

  def roleOption(role: String): Option[String] = role match {
    case null | "" => None
    case _         => Some(role)
  }

  private val entityToSchema = new mutable.HashMap[String, Schema]()
  private val schemasLock = new ReentrantReadWriteLock()

  /**
   * TODO support multiple versions of schema
   */
  private def putSchema(system: ActorSystem, entityName: String, schema: Schema): Unit =
    try {
      schemasLock.writeLock.lock
      entityToSchema.get(entityName) match {
        case Some(`schema`) => // existed, do nothing, or upgrade to new schema ? TODO
        case _ =>
          entityToSchema(entityName) = schema
          Entity.startSharding(system, entityName, Some(Entity.props(entityName, schema, RecordBuilder(schema))))
      }
    } finally {
      schemasLock.writeLock.unlock
    }

  private def removeSchema(entityName: String): Unit =
    try {
      schemasLock.writeLock.lock
      entityToSchema -= entityName
      // TODO stop sharding, remove all actor instances of thie entity
    } finally {
      schemasLock.writeLock.unlock
    }

  def schemaOf(entityName: String): Option[Schema] =
    try {
      schemasLock.readLock.lock
      entityToSchema.get(entityName)
    } finally {
      schemasLock.readLock.unlock
    }

}

class DistributedSchemaBoard(
    val role: Option[String],
    val gossipInterval: FiniteDuration,
    val removedTimeToLive: FiniteDuration,
    val maxDeltaElements: Int) extends DistributedStatusBoard[String] {

  import context.dispatcher

  def receive = businessReceive orElse generalReceive

  def businessReceive: Receive = {
    case astore.PutSchema(entityName, schemaStr, Some(fullName)) =>
      val commander = sender()
      parseSchema(schemaStr) match {
        case Success(unionSchema) =>
          if (unionSchema.getType == Schema.Type.UNION) {
            val schema = unionSchema.getTypes.get(unionSchema.getIndexNamed(fullName))
            if (schema != null) {
              self.ask(Put(entityName, schema.toString))(5.seconds).mapTo[PutAck].onComplete {
                case Success(ack) => commander ! Success(entityName)
                case failure      => commander ! failure
              }
            } else {
              commander ! Failure(new RuntimeException("Can not parse the " + fullName))
            }
          } else {
            commander ! Failure(new RuntimeException("Schema with full name [" + fullName + "] should be Union type"))
          }
        case failure => commander ! failure
      }

    case astore.PutSchema(entityName, schemaStr, None) =>
      val commander = sender()
      parseSchema(schemaStr) match {
        case Success(schema) =>
          self.ask(Put(entityName, schema.toString))(5.seconds).mapTo[PutAck].onComplete {
            case Success(ack) => commander ! Success(entityName)
            case failure      => commander ! failure
          }
        case failure => commander ! failure
      }

    case astore.RemoveSchema(entityName) =>
      val commander = sender()
      self.ask(Remove(entityName))(5.seconds).mapTo[RemoveAck].onComplete {
        case Success(ack) => commander ! Success(entityName)
        case failure      => commander ! failure
      }
  }

  private def parseSchema(schema: String) = {
    try {
      val res = new Schema.Parser().parse(schema)
      Success(res)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

  override def onPut(keys: Set[String]) {
    for {
      (owner, bucket) <- registry
      (entityName, valueHolder) <- bucket.content if keys.contains(entityName)
    } {
      valueHolder.value match {
        case Some(schemaStr) =>
          log.info("put schema [{}]:\n{} ", entityName, schemaStr)
          try {
            val schema = new Schema.Parser().parse(schemaStr)
            DistributedSchemaBoard.putSchema(context.system, entityName, schema)
          } catch {
            case ex: Throwable => log.error(ex, ex.getMessage)
          }
        case None =>
          log.info("remove schemas: {}", keys)
          keys foreach DistributedSchemaBoard.removeSchema
      }
    }
  }

  override def onRemoved(keys: Set[String]) {
    log.info("remove schemas: {}", keys)
    keys foreach DistributedSchemaBoard.removeSchema
  }

}

class DistributedSchemaBoardExtension(system: ExtendedActorSystem) extends Extension {

  private val config = system.settings.config.getConfig("wandou.astore.schema-board")
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
   * The [[DistributedSchemaBoard]]
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
        DistributedSchemaBoard.props(role, gossipInterval, removedTimeToLive, maxDeltaElements),
        name)
    }
  }
}

