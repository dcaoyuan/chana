package wandou.astore.schema

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.actor.Props
import akka.contrib.datareplication.DataReplication
import akka.contrib.datareplication.LWWMap
import akka.pattern.ask
import akka.cluster.Cluster
import java.util.concurrent.locks.ReentrantReadWriteLock
import org.apache.avro.Schema
import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import wandou.astore
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
  def props(): Props = Props(classOf[DistributedSchemaBoard])

  private val entityToSchema = new mutable.HashMap[String, Schema]()
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
          entityToSchema(entityName) = schema
          Entity.startSharding(system, entityName, Some(Entity.props(entityName, schema, RecordBuilder(schema), idleTimeout)))
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

  val DataKey = "astore-schemas"

}

class DistributedSchemaBoard extends Actor with ActorLogging {
  import akka.contrib.datareplication.Replicator._

  val replicator = DataReplication(context.system).replicator

  implicit val cluster = Cluster(context.system)
  import context.dispatcher

  replicator ! Subscribe(DistributedSchemaBoard.DataKey, self)

  def receive = {
    case astore.PutSchema(entityName, schemaStr, entityFullName, idleTimeout) =>
      val commander = sender()
      parseSchema(schemaStr) match {
        case Success(parsedSchema) =>
          val aschema = entityFullName match {
            case Some(fullName) =>
              if (parsedSchema.getType == Schema.Type.UNION) {
                val x = parsedSchema.getTypes.get(parsedSchema.getIndexNamed(fullName))
                if (x != null) {
                  Left(x)
                } else {
                  Right("Schema with full name [" + fullName + "] cannot be found")
                }
              } else {
                Right("Schema with full name [" + fullName + "] should be Union type")
              }
            case None => Left(parsedSchema)
          }
          aschema match {
            case Left(schema) =>
              val key = entityName

              replicator.ask(Update(DistributedSchemaBoard.DataKey, LWWMap(), WriteLocal)(_ + (key -> (schema.toString, idleTimeout))))(60.seconds).onComplete {
                case Success(_: UpdateSuccess) =>
                  log.info("put schema [{}]:\n{} ", key, schemaStr)
                  DistributedSchemaBoard.putSchema(context.system, key, schema, idleTimeout)
                  // TODO wait for sharding ready
                  commander ! Success(key)
                case Success(_: UpdateTimeout) => commander ! Failure(new RuntimeException("Update timeout"))
                case Success(x: InvalidUsage)  => commander ! Failure(x)
                case Success(x: ModifyFailure) => commander ! Failure(x)
                case failure                   => commander ! failure
              }
            case Right(reason) => commander ! Failure(new RuntimeException(reason))
          }
        case failure => commander ! failure
      }

    case astore.RemoveSchema(entityName) =>
      val commander = sender()
      val key = entityName

      replicator.ask(Update(DistributedSchemaBoard.DataKey, LWWMap(), WriteLocal)(_ - key))(60.seconds).onComplete {
        case Success(_: UpdateSuccess) =>
          log.info("remove schemas: {}", key)
          DistributedSchemaBoard.removeSchema(key)
          // TODO wait for stop sharding? 
          commander ! Success(key)
        case Success(_: UpdateTimeout) => commander ! Failure(new RuntimeException("Update timeout"))
        case Success(x: InvalidUsage)  => commander ! Failure(x)
        case Success(x: ModifyFailure) => commander ! Failure(x)
        case failure                   => commander ! failure
      }

    case Changed(DistributedSchemaBoard.DataKey, LWWMap(entries: Map[String, (String, Duration)] @unchecked)) =>
      // check if there were newly added
      entries.foreach {
        case (entityName, (schemaStrx, idleTimeout)) =>
          DistributedSchemaBoard.entityToSchema.get(entityName) match {
            case None =>
              parseSchema(schemaStrx) match {
                case Success(schema) =>
                  log.info("put schema [{}]:\n{} ", entityName, schemaStrx)
                  DistributedSchemaBoard.putSchema(context.system, entityName, schema, idleTimeout)
                case Failure(ex) =>
                  log.error(ex, ex.getMessage)
              }
            case Some(schema) => // TODO, existed, but changed?
          }
      }

      // check if there were removed
      val toRemove = DistributedSchemaBoard.entityToSchema.filter(x => !entries.contains(x._1)).keys
      if (toRemove.nonEmpty) {
        log.info("remove schemas: {}", toRemove)
        toRemove foreach DistributedSchemaBoard.removeSchema
      }
  }

  private def parseSchema(schema: String) =
    try {
      val res = new Schema.Parser().parse(schema)
      Success(res)
    } catch {
      case ex: Throwable => Failure(ex)
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
