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
 */
final case class PutSchema(entityName: String, schema: Schema)
final case class DelSchema(entityName: String)

object SchemaBoard {
  private val entityToSchema = new mutable.HashMap[String, Schema]()
  private val schemasLock = new ReentrantReadWriteLock()

  private def putSchema(system: ActorSystem, entityName: String, schema: Schema): Unit =
    try {
      schemasLock.writeLock.lock
      entityToSchema.get(entityName) match {
        case Some(`schema`) => // existed, do nothing
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
  private var singletons: SystemSingletons = _
  private val singletonsMutex = new AnyRef()

  final class SystemSingletons(system: ActorSystem) {
    lazy val schemaBoard = system.actorOf(Props(new SchemaBoard), SCHEMA_BOARD)
  }

  def apply(system: ActorSystem): SystemSingletons = {
    if (singletons eq null) {
      singletonsMutex synchronized {
        if (singletons eq null) {
          singletons = new SystemSingletons(system)
        }
      }
    }
    singletons
  }
}

class SchemaBoard extends Actor with ActorLogging {
  def receive = {
    case PutSchema(entityName, schema) =>
      try {
        SchemaBoard.putSchema(context.system, entityName, schema)
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
