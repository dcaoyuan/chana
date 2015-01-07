package wandou.astore.schema

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.Props
import akka.contrib.pattern.DistributedPubSubExtension
import akka.contrib.pattern.DistributedPubSubMediator
import java.util.concurrent.locks.ReentrantReadWriteLock
import org.apache.avro.Schema
import scala.collection.mutable
import scala.util.Failure
import scala.util.Success
import wandou.astore.Entity
import wandou.avro.RecordBuilder

/**
 * @param   entity name
 * @param   schema of entity
 * @param   the full name (with namespace) of this entity, in case of there are
 *          embbed complex types which have to be defined in union in one schema file
 */
final case class PutSchema(entityName: String, schema: Schema, entityFullName: Option[String] = None)
final case class DelSchema(entityName: String)

object NodeSchemaBoard {
  private val entityToSchema = new mutable.HashMap[String, Schema]()
  private val schemasLock = new ReentrantReadWriteLock()

  private def putSchema(system: ActorSystem, entityName: String, schema: Schema, entityFullName: Option[String] = None): Unit =
    try {
      schemasLock.writeLock.lock
      entityToSchema.get(entityName) match {
        case Some(`schema`) => // existed, do nothing, or upgrade to new schema ? TODO
        case _ =>
          entityToSchema(entityName) = schema
          Entity.startSharding(system, entityName, Some(Entity.props(schema, RecordBuilder(schema))))
      }
    } finally {
      schemasLock.writeLock.unlock
    }

  private def delSchema(entityName: String): Unit =
    try {
      schemasLock.writeLock.lock
      entityToSchema -= entityName
      // TODO remove all actor instances of thie entity
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

  val SCHEMA_BOARD = "schemaBoard"
  private var singletons: NodeSingletons = _
  private val singletonsMutex = new AnyRef()

  final class NodeSingletons(system: ActorSystem) {
    val schemaBoard = system.actorOf(Props(classOf[NodeSchemaBoard]), SCHEMA_BOARD)
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

class NodeSchemaBoard extends Actor with ActorLogging {
  log.info("{} started.", self.path)

  val mediator = DistributedPubSubExtension(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

  def receive = {
    case x @ PutSchema(entityName, schema, entityFullName) =>
      log.info("Got: {}", x)
      try {
        NodeSchemaBoard.putSchema(context.system, entityName, schema, entityFullName)
        sender() ! Success(entityName)
      } catch {
        case ex: Throwable =>
          log.error(ex, ex.getMessage)
          sender() ! Failure(ex)
      }
    case x @ DelSchema(entityName) =>
      log.info("Got: {}", x)
      try {
        NodeSchemaBoard.delSchema(entityName)
        sender() ! Success(entityName)
      } catch {
        case ex: Throwable =>
          log.error(ex, ex.getMessage)
          sender() ! Failure(ex)
      }
  }
}
