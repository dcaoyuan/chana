package wandou.avds.schema

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.Props
import java.util.concurrent.locks.ReentrantReadWriteLock
import org.apache.avro.Schema
import scala.collection.mutable
import scala.util.Failure
import scala.util.Success

/**
 * @param   entity name
 * @param   schema of entity
 */
final case class PutSchema(entityName: String, schema: Schema)
final case class DelSchema(entityName: String)

object SchemaBoard {
  private val entityToSchema = new mutable.HashMap[String, Schema]()
  private val scriptsLock = new ReentrantReadWriteLock()

  private def putSchema(entity: String, schema: Schema): Unit =
    try {
      scriptsLock.writeLock.lock
      entityToSchema(entity) = schema
    } finally {
      scriptsLock.writeLock.unlock
    }

  private def delSchema(entity: String): Unit =
    try {
      scriptsLock.writeLock.lock
      entityToSchema -= entity
    } finally {
      scriptsLock.writeLock.unlock
    }

  def schemaOf(entity: String) =
    try {
      scriptsLock.readLock.lock
      entityToSchema.get(entity)
    } finally {
      scriptsLock.readLock.unlock
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
        SchemaBoard.putSchema(entityName, schema)
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
