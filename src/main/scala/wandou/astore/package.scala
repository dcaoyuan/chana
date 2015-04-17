package wandou

import org.apache.avro.generic.GenericData.Record
import scala.concurrent.duration.Duration

package object astore {

  trait Command extends Serializable {
    def id: String
  }

  trait Event extends Serializable

  final case class GetRecord(id: String) extends Command
  final case class GetRecordAvro(id: String) extends Command
  final case class GetRecordJson(id: String) extends Command
  final case class PutRecord(id: String, record: Record) extends Command
  final case class PutRecordJson(id: String, record: String) extends Command
  final case class GetField(id: String, field: String) extends Command
  final case class GetFieldAvro(id: String, field: String) extends Command
  final case class GetFieldJson(id: String, field: String) extends Command
  final case class PutField(id: String, field: String, value: Any) extends Command
  final case class PutFieldJson(id: String, field: String, value: String) extends Command

  final case class Select(id: String, path: String) extends Command
  final case class SelectAvro(id: String, path: String) extends Command
  final case class SelectJson(id: String, path: String) extends Command
  final case class Update(id: String, path: String, value: Any) extends Command
  final case class UpdateJson(id: String, path: String, value: String) extends Command
  final case class Insert(id: String, path: String, value: Any) extends Command
  final case class InsertJson(id: String, path: String, value: String) extends Command
  final case class InsertAll(id: String, path: String, values: java.util.Collection[_]) extends Command
  final case class InsertAllJson(id: String, path: String, values: String) extends Command
  final case class Delete(id: String, path: String) extends Command
  final case class Clear(id: String, path: String) extends Command

  /**
   * @param   entity name
   * @param   schema of entity
   */
  final case class PutSchema(entityName: String, schema: String, entityFullName: Option[String], idleTimeout: Duration) extends Event
  final case class RemoveSchema(entityName: String) extends Event

  /**
   * @param   entity name
   * @param   script id
   * @param   JavaScript code in string
   */
  final case class PutScript(entity: String, field: String, id: String, script: String)
  final case class RemoveScript(entity: String, field: String, id: String)

  final case class UpdatedFields(updatedFields: List[(Int, Any)]) extends Event

  object UpdateTimeoutException extends RuntimeException("Update timeout")
}
