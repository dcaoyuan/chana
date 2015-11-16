package chana.schema

import akka.actor._
import akka.cluster.Cluster
import akka.contrib.datareplication.{ DataReplication, LWWMap }
import akka.contrib.pattern.ClusterSharding
import akka.pattern.ask
import akka.persistence._
import chana.{ Entity, Event, PutSchema, RemoveSchema }
import chana.avro.DefaultRecordBuilder
import java.net.URLEncoder
import java.util.concurrent.ConcurrentHashMap
import org.apache.avro.Schema
import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.util.{ Failure, Success }

trait SchemaBoard {
  def schemaOf(entityName: String): Option[Schema]
}

/**
 * Extension that starts a [[DistributedSchemaBoard]] actor
 * with settings defined in config section `chana.schema-board`.
 */
object DistributedSchemaBoard extends ExtensionId[DistributedSchemaBoardExtension] with ExtensionIdProvider with SchemaBoard {
  // -- implementation of akka extention 
  override def get(system: ActorSystem) = super.get(system)
  override def lookup = DistributedSchemaBoard
  override def createExtension(system: ExtendedActorSystem) = new DistributedSchemaBoardExtension(system)
  // -- end of implementation of akka extention 

  /**
   * Scala API: Factory method for `DistributedSchemaBoard` [[akka.actor.Props]].
   */
  def props(): Props = Props(classOf[DistributedSchemaBoard])

  private val entityToSchema = new ConcurrentHashMap[String, (Schema, Duration)]()

  /**
   * TODO support multiple versions of schema
   */
  private def putSchema(system: ActorSystem, entityName: String, schema: Schema, idleTimeout: Duration = Duration.Undefined): Unit = {
    val entityname = entityName.toLowerCase
    entityToSchema.putIfAbsent(entityname, (schema, idleTimeout)) match {
      case null =>
        Entity.startSharding(system, entityname, Some(Entity.props(entityname, schema, DefaultRecordBuilder(schema), idleTimeout)))
      case old => // If existed, do nothing, or upgrade to new schema ? TODO
    }
  }

  private def removeSchema(system: ActorSystem, entityName: String): Unit = {
    val entityname = entityName.toLowerCase
    entityToSchema.remove(entityname)
    removeClusterShardActor(system, entityname)
  }

  private def removeClusterShardActor(system: ActorSystem, entityName: String): Unit = {
    try {
      val regionRef = ClusterSharding(system).shardRegion(entityName)
      regionRef ! Kill

      val encName = URLEncoder.encode(entityName, "utf-8")
      val coordinatorSingletonManagerName = encName + "Coordinator"
      val coordinatorPath = (regionRef.path.parent / coordinatorSingletonManagerName / "singleton" / "coordinator").toStringWithoutAddress
      val coordRef = system.actorSelection(coordinatorPath)
      coordRef ! Kill
    } finally {
    }
  }

  def schemaOf(entityName: String): Option[Schema] = {
    Option(entityToSchema.get(entityName.toLowerCase)).map(_._1)
  }

  val DataKey = "chana--schemas"

}

class DistributedSchemaBoard extends Actor with ActorLogging with PersistentActor {
  import akka.contrib.datareplication.Replicator._

  override def persistenceId: String = "distributed_schema_board"

  implicit val cluster = Cluster(context.system)
  import context.dispatcher

  val replicator = DataReplication(context.system).replicator
  replicator ! Subscribe(DistributedSchemaBoard.DataKey, self)

  val persistent = context.system.settings.config.getBoolean("chana.persistence.persistent")

  override def receiveRecover: Receive = {
    case SnapshotOffer(metadata, offeredSnapshot: Map[String, (String, Duration)] @unchecked) =>
      // TODO leveldb plugin does not provide the lastest timestamped snapshot, why?
      log.debug("SnapshotOffer: {}", offeredSnapshot)
      offeredSnapshot foreach {
        case (entityName, (schemaStr, idleTimeout)) =>
          parseSchema(schemaStr) match {
            case Success(schema) => putSchema(entityName, schema, idleTimeout, commander = None, alsoSave = false)
            case Failure(ex)     => log.error("Snapshot offer failed to parse: {}", ex)
          }
      }
    case event: Event           => updateSchema(event)

    case x: SnapshotOffer       => log.warning("Got unknown SnapshotOffer: {}", x)
    case RecoveryFailure(cause) => log.error("Recovery failure: {}", cause)
    case RecoveryCompleted      => log.debug("Recovery completed: {}", persistenceId)
  }

  override def receiveCommand = {
    case chana.PutSchema(entityName, schemaStr, entityFullName, idleTimeout) =>
      val commander = sender()
      parseSchema(schemaStr) match {
        case Success(parsedSchema) =>
          val trySchema = entityFullName match {
            case Some(fullName) =>
              parsedSchema.getType match {
                case Schema.Type.UNION =>
                  val entrySchema = parsedSchema.getTypes.get(parsedSchema.getIndexNamed(fullName))
                  if (entrySchema != null) {
                    Success(entrySchema)
                  } else {
                    Failure(new RuntimeException("Schema with full name [" + fullName + "] cannot be found"))
                  }
                case _ => Failure(new RuntimeException("Schema with full name [" + fullName + "] should be Union type"))
              }
            case None => Success(parsedSchema)
          }
          trySchema match {
            case Success(schema) => putSchema(entityName, schema, idleTimeout, Some(commander), alsoSave = true)
            case failure         => commander ! failure
          }
        case failure => commander ! failure
      }

    case chana.RemoveSchema(entityName) =>
      val commander = sender()
      removeSchema(entityName, Some(commander), alsoSave = true)

    case Changed(DistributedSchemaBoard.DataKey, LWWMap(entries: Map[String, (String, Duration)] @unchecked)) =>
      // check if there were newly added
      entries.foreach {
        case (entityName, (schemaStr, idleTimeout)) =>
          DistributedSchemaBoard.entityToSchema.get(entityName) match {
            case null =>
              parseSchema(schemaStr) match {
                case Success(schema) =>
                  log.info("put schema (Changed) [{}]:\n{} ", entityName, schemaStr)
                  DistributedSchemaBoard.putSchema(context.system, entityName, schema, idleTimeout)
                case Failure(ex) =>
                  log.error(ex, ex.getMessage)
              }
            case _ => // TODO, existed, but changed? should we remove all records according to the obsolete schema?
          }
      }

      // check if there were removed
      val toRemove = DistributedSchemaBoard.entityToSchema.filter(x => !entries.contains(x._1)).keys
      if (toRemove.nonEmpty) {
        log.info("remove schemas (Changed) [{}]", toRemove)
        toRemove.foreach(DistributedSchemaBoard.removeSchema(context.system, _))
      }

    case f: PersistenceFailure                 => log.error("{}", f)
    case SaveSnapshotSuccess(metadata)         => log.debug("success saved {}", metadata)
    case SaveSnapshotFailure(metadata, reason) => log.error("Failed to save snapshot {}: {}", metadata, reason)
  }

  private def parseSchema(schema: String) =
    try {
      val res = new Schema.Parser().parse(schema)
      Success(res)
    } catch {
      case ex: Throwable => Failure(ex)
    }

  private def putSchema(entityName: String, schema: Schema, idleTimeout: Duration, commander: Option[ActorRef], alsoSave: Boolean): Unit = {
    val key = entityName
    replicator.ask(Update(DistributedSchemaBoard.DataKey, LWWMap(), WriteAll(60.seconds))(_ + (key -> (schema.toString, idleTimeout))))(60.seconds).onComplete {
      case Success(_: UpdateSuccess) =>
        log.info("put schema (Update) [{}]:\n{} ", key, schema)
        DistributedSchemaBoard.putSchema(context.system, entityName, schema, idleTimeout)
        // for schema, just save snapshot
        if (persistent && alsoSave) {
          doSnapshot()
        }

        // TODO wait for sharding ready
        commander.foreach(_ ! Success(key))

      case Success(_: UpdateTimeout) => commander.foreach(_ ! Failure(chana.UpdateTimeoutException))
      case Success(x: InvalidUsage)  => commander.foreach(_ ! Failure(x))
      case Success(x: ModifyFailure) => commander.foreach(_ ! Failure(x))
      case Success(x)                => log.warning("Got {}", x)
      case ex: Failure[_]            => commander.foreach(_ ! ex)
    }
  }

  private def removeSchema(entityName: String, commander: Option[ActorRef], alsoSave: Boolean): Unit = {
    val key = entityName
    replicator.ask(Update(DistributedSchemaBoard.DataKey, LWWMap(), WriteAll(60.seconds))(_ - key))(60.seconds).onComplete {
      case Success(_: UpdateSuccess) =>
        log.info("remove schemas (Update): {}", key)
        DistributedSchemaBoard.removeSchema(context.system, entityName)
        // for schema, just save snapshot
        if (persistent && alsoSave) {
          doSnapshot()
        }

        // TODO wait for sharding stopped?
        commander.foreach(_ ! Success(key))

      case Success(_: UpdateTimeout) => commander.foreach(_ ! Failure(chana.UpdateTimeoutException))
      case Success(x: InvalidUsage)  => commander.foreach(_ ! Failure(x))
      case Success(x: ModifyFailure) => commander.foreach(_ ! Failure(x))
      case Success(x)                => log.warning("Got {}", x)
      case ex: Failure[_]            => commander.foreach(_ ! ex)
    }
  }

  private def updateSchema(ev: Event): Unit = {
    ev match {
      case PutSchema(entityName, schemaStr, _, idleTimeout) =>
        parseSchema(schemaStr) match {
          case Success(schema) => putSchema(entityName, schema, idleTimeout, commander = None, alsoSave = false)
          case Failure(ex)     => log.error("{}", ex.getCause)
        }
      case RemoveSchema(entityName) => removeSchema(entityName, commander = None, alsoSave = false)
      case _                        => log.error("unknown schema persistence event")
    }
  }

  private def doSnapshot(): Unit = {
    val schemas = DistributedSchemaBoard.entityToSchema.map {
      case (entityName, (schema, timeout)) => (entityName, (schema.toString, timeout))
    }.toMap
    saveSnapshot(schemas)
  }
}

class DistributedSchemaBoardExtension(system: ExtendedActorSystem) extends Extension {

  private val config = system.settings.config.getConfig("chana.schema-board")
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
