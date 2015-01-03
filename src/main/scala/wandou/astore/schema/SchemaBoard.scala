package wandou.astore.schema

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.Props
import java.util.concurrent.locks.ReentrantReadWriteLock
import org.apache.avro.Schema
import scala.collection.mutable
import scala.util.Failure
import scala.util.Success
import wandou.astore.Entity

/**
 * @param   entity name
 * @param   schema of entity
 * @param   the full name (with namespace) of this entity, in case of there are
 *          embbed complex types which have to be defined in union in one schema file
 */
final case class PutSchema(entityName: String, schema: Schema, entityFullName: Option[String] = None)
final case class DelSchema(entityName: String)

object SchemaBoard {
  private val entityToSchema = new mutable.HashMap[String, Schema]()
  private val schemasLock = new ReentrantReadWriteLock()

  private def putSchema(system: ActorSystem, entityName: String, schema: Schema, entityFullName: Option[String] = None): Unit =
    try {
      schemasLock.writeLock.lock
      entityToSchema.get(entityName) match {
        case Some(`schema`) => // existed, do nothing, or upgrade to new schema ? TODO
        case _ =>
          Entity.startSharding(system, entityName, Some(Entity.props(schema)))
          entityToSchema(entityName) = schema
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

  def schemaOf(entity: String): Option[Schema] =
    try {
      schemasLock.readLock.lock
      entityToSchema.get(entity)
    } finally {
      schemasLock.readLock.unlock
    }

  val SCHEMA_BOARD = "schemaBoard"
  private var singletons: NodeSingletons = _
  private val singletonsMutex = new AnyRef()

  final class NodeSingletons(system: ActorSystem) {
    lazy val schemaBoard = system.actorOf(Props(new SchemaBoard), SCHEMA_BOARD)
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

class SchemaBoard extends Actor with ActorLogging {
  def receive = {
    case PutSchema(entityName, schema, entityFullName) =>
      try {
        SchemaBoard.putSchema(context.system, entityName, schema, entityFullName)
        sender() ! Success(entityName)
      } catch {
        case ex: Throwable =>
          log.error(ex, ex.getMessage)
          sender() ! Failure(ex)
      }
    case DelSchema(entityName) =>
      try {
        SchemaBoard.delSchema(entityName)
        sender() ! Success(entityName)
      } catch {
        case ex: Throwable =>
          log.error(ex, ex.getMessage)
          sender() ! Failure(ex)
      }
  }
}
