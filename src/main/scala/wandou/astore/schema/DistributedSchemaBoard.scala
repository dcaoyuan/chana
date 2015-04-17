package wandou.astore.schema

import java.util.concurrent.locks.ReentrantReadWriteLock

import akka.actor.{ Actor, ActorLogging, ActorRef, ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, Props }
import akka.cluster.Cluster
import akka.contrib.datareplication.{ DataReplication, LWWMap }
import akka.pattern.ask
import akka.persistence._
import org.apache.avro.Schema
import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.{ Failure, Success }
import wandou.astore
import wandou.astore.{ Entity, Event }
import wandou.astore.RemoveSchema
import wandou.astore.PutSchema
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
  def props(): Props = Props(classOf[DistributedSchemaBoard])

  private val entityToSchema = new mutable.HashMap[String, (Schema, Duration)]()
  private val schemasLock = new ReentrantReadWriteLock()

  /**
   * TODO support multiple versions of schema
   */
  private def putSchema(system: ActorSystem, entityName: String, schema: Schema, idleTimeout: Duration = Duration.Undefined): Unit =
    try {
      schemasLock.writeLock.lock
      entityToSchema.get(entityName) match {
        case Some(`schema`) => // existed, do nothing, or upgrade to new schema ? TODO
        case _ =>
          entityToSchema(entityName) = (schema, idleTimeout)
          Entity.startSharding(system, entityName, Some(Entity.props(entityName, schema, RecordBuilder(schema), idleTimeout)))
      }
    } finally {
      schemasLock.writeLock.unlock
    }

  private def removeSchema(entityName: String): Unit =
    try {
      schemasLock.writeLock.lock
      entityToSchema -= entityName
      // TODO stop sharding, remove all actor instances of the entity
    } finally {
      schemasLock.writeLock.unlock
    }

  def schemaOf(entityName: String): Option[Schema] =
    try {
      schemasLock.readLock.lock
      entityToSchema.get(entityName).map(_._1)
    } finally {
      schemasLock.readLock.unlock
    }

  val DataKey = "astore-schemas"

}

class DistributedSchemaBoard extends Actor with ActorLogging with PersistentActor {
  import akka.contrib.datareplication.Replicator._

  override def persistenceId: String = self.path.name

  val replicator = DataReplication(context.system).replicator

  implicit val cluster = Cluster(context.system)
  import context.dispatcher

  replicator ! Subscribe(DistributedSchemaBoard.DataKey, self)

  override def receiveRecover: Receive = {
    case SnapshotOffer(metadata, offeredSnapshot: Map[String, (String, Duration)] @unchecked) =>
      offeredSnapshot foreach {
        case (entityName, (schemaStr, idleTimeout)) =>
          parseSchema(schemaStr) match {
            case Success(schema) =>
              putSchema(entityName, schema, idleTimeout, None)
            case failure => log.error("Snapshot offer failed to parse")
          }
      }
    case event: Event           => updateSchema(event)
    case RecoveryFailure(cause) => log.error("Recovery failure: {}", cause)
    case RecoveryCompleted      => log.debug("Recovery completed: {}", persistenceId)
  }

  override def receiveCommand = {
    case astore.PutSchema(entityName, schemaStr, entityFullName, idleTimeout) =>
      val commander = sender()
      parseSchema(schemaStr) match {
        case Success(parsedSchema) =>
          val trySchema = entityFullName match {
            case Some(fullName) =>
              if (parsedSchema.getType == Schema.Type.UNION) {
                val x = parsedSchema.getTypes.get(parsedSchema.getIndexNamed(fullName))
                if (x != null) {
                  Success(x)
                } else {
                  Failure(new RuntimeException("Schema with full name [" + fullName + "] cannot be found"))
                }
              } else {
                Failure(new RuntimeException("Schema with full name [" + fullName + "] should be Union type"))
              }
            case None => Success(parsedSchema)
          }
          trySchema match {
            case Success(schema) => putSchema(entityName, schema, idleTimeout, Some(commander))
            case failure         => commander ! failure
          }
        case failure => commander ! failure
      }

    case astore.RemoveSchema(entityName) =>
      val commander = sender()
      removeSchema(entityName, Some(commander))

    case Changed(DistributedSchemaBoard.DataKey, LWWMap(entries: Map[String, (String, Duration)] @unchecked)) =>
      // check if there were newly added
      entries.foreach {
        case (entityName, (schemaStr, idleTimeout)) =>
          DistributedSchemaBoard.entityToSchema.get(entityName) match {
            case None =>
              parseSchema(schemaStr) match {
                case Success(schema) =>
                  log.info("put schema [{}]:\n{} ", entityName, schemaStr)
                  DistributedSchemaBoard.putSchema(context.system, entityName, schema, idleTimeout)
                case Failure(ex) =>
                  log.error(ex, ex.getMessage)
              }
            case Some(schema) => // TODO, existed, but changed? should we remove all records according to the obsolete schema?
          }
      }

      // check if there were removed
      val toRemove = DistributedSchemaBoard.entityToSchema.filter(x => !entries.contains(x._1)).keys
      if (toRemove.nonEmpty) {
        log.info("remove schemas: {}", toRemove)
        toRemove foreach DistributedSchemaBoard.removeSchema
      }

    case f: PersistenceFailure  => log.error("{}", f)
    case f: SaveSnapshotFailure => log.error("{}", f)
  }

  private def parseSchema(schema: String) =
    try {
      val res = new Schema.Parser().parse(schema)
      Success(res)
    } catch {
      case ex: Throwable => Failure(ex)
    }

  private def putSchema(entityName: String, schema: Schema, idleTimeout: Duration, commander: Option[ActorRef]): Unit = {
    val key = entityName
    replicator.ask(Update(DistributedSchemaBoard.DataKey, LWWMap(), WriteAll(60.seconds))(_ + (key -> (schema.toString, idleTimeout))))(60.seconds).onComplete {
      case Success(_: UpdateSuccess) =>
        log.info("put schema [{}]:\n{} ", key, schema)
        DistributedSchemaBoard.putSchema(context.system, entityName, schema, idleTimeout)
        // for schema, just save snapshot
        doSnapshot()

        // TODO wait for sharding ready
        commander.foreach(_ ! Success(key))
      case Success(_: UpdateTimeout) => commander.foreach(_ ! Failure(astore.UpdateTimeoutException))
      case Success(x: InvalidUsage)  => commander.foreach(_ ! Failure(x))
      case Success(x: ModifyFailure) => commander.foreach(_ ! Failure(x))
      case failure                   => commander.foreach(_ ! failure)
    }
  }

  private def removeSchema(entityName: String, commander: Option[ActorRef]): Unit = {
    val key = entityName
    replicator.ask(Update(DistributedSchemaBoard.DataKey, LWWMap(), WriteAll(60.seconds))(_ - key))(60.seconds).onComplete {
      case Success(_: UpdateSuccess) =>
        log.info("remove schemas: {}", key)
        DistributedSchemaBoard.removeSchema(entityName)
        // for schema, just save snapshot
        doSnapshot()

        // TODO wait for sharding stopped?
        commander.foreach(_ ! Success(key))
      case Success(_: UpdateTimeout) => commander.foreach(_ ! Failure(astore.UpdateTimeoutException))
      case Success(x: InvalidUsage)  => commander.foreach(_ ! Failure(x))
      case Success(x: ModifyFailure) => commander.foreach(_ ! Failure(x))
      case failure                   => commander.foreach(_ ! failure)
    }
  }

  private def updateSchema(ev: Event): Unit = {
    ev match {
      case PutSchema(entityName, schemaStr, _, idleTimeout) =>
        parseSchema(schemaStr) match {
          case Success(schema) => putSchema(entityName, schema, idleTimeout, None)
          case Failure(ex)     => log.error("{}", ex.getCause)
        }
      case RemoveSchema(entityName) => removeSchema(entityName, None)
      case _                        => log.error("unknown schema persistence event")
    }
  }

  private def doSnapshot(): Unit = {
    saveSnapshot(DistributedSchemaBoard.entityToSchema.map {
      case (entityName, (schema, timeout)) => (entityName, (schema.toString, timeout))
    }.toMap)
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
      val name = config.getString("name")
      system.actorOf(
        DistributedSchemaBoard.props(),
        name)
    }
  }
}
