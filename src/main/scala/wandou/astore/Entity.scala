package wandou.astore

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.ReceiveTimeout
import akka.actor.Stash
import akka.contrib.pattern.ClusterSharding
import akka.contrib.pattern.ShardRegion
import akka.persistence.PersistenceFailure
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.Record
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import wandou.astore.script.Scriptable
import wandou.avpath
import wandou.avpath.Evaluator
import wandou.avpath.Evaluator.Ctx
import wandou.avro
import wandou.avro.RecordBuilder

object Entity {
  def props(name: String, schema: Schema, builder: RecordBuilder) = Props(classOf[Entity], name, schema, builder)

  lazy val idExtractor: ShardRegion.IdExtractor = {
    case cmd: Command => (cmd.id, cmd)
  }

  lazy val shardResolver: ShardRegion.ShardResolver = {
    case cmd: Command => (cmd.id.hashCode % 100).toString
  }

  def startSharding(system: ActorSystem, shardName: String, entryProps: Option[Props]) = {
    ClusterSharding(system).start(
      typeName = shardName,
      entryProps = entryProps,
      idExtractor = idExtractor,
      shardResolver = shardResolver)
  }

  final case class OnUpdated(id: String, fieldsBefore: Array[(Schema.Field, Any)], recordAfter: Record)

  private[astore] object Internal {
    final case class Bootstrap(val record: Record)
  }
}

class Entity(val name: String, val schema: Schema, val builder: RecordBuilder) extends EntityActor with Scriptable {
  override def ready = accessBehavior orElse scriptableBehavior
}

trait EntityActor extends Actor with Stash with ActorLogging {
  import Entity.Internal._

  import context.dispatcher

  def schema: Schema
  def builder: RecordBuilder

  private val id = self.path.name
  private val parser = new avpath.Parser()
  private val encoderDecoder = new avro.EncoderDecoder()
  private var limitedSize = 30 // TODO

  private var record: Record = _
  private def createRecord() {
    record = builder.build()
  }

  override def preStart {
    super[Actor].preStart
    log.debug("Starting: {} ", id)
    //context.setReceiveTimeout(ReceiveTimeoutSecs)
    createRecord()
    // TODO load persistented data
    self ! Bootstrap(record)
  }

  override def receive: Receive = initial

  def initial: Receive = {
    case Bootstrap(r) =>
      record = r
      context.become(ready)
      unstashAll()
    case _ =>
      stash()
  }

  def ready = accessBehavior

  def accessBehavior: Receive = {
    case GetRecord(_) =>
      sender() ! Success(Ctx(record, schema, null))

    case GetRecordAvro(_) =>
      sender() ! encoderDecoder.avroEncode(record, schema)

    case GetRecordJson(_) =>
      sender() ! encoderDecoder.jsonEncode(record, schema)

    case GetField(_, fieldName) =>
      val commander = sender()
      val field = schema.getField(fieldName)
      if (field != null) {
        commander ! Success(Ctx(record.get(field.pos), field.schema, field))
      } else {
        val ex = new RuntimeException("Field does not exist: " + fieldName)
        log.error(ex, ex.getMessage)
        commander ! Failure(ex)
      }

    case GetFieldAvro(_, fieldName) =>
      val commander = sender()
      val field = schema.getField(fieldName)
      if (field != null) {
        commander ! encoderDecoder.avroEncode(record.get(field.pos), field.schema)
      } else {
        val ex = new RuntimeException("Field does not exist: " + fieldName)
        log.error(ex, ex.getMessage)
        commander ! Failure(ex)
      }

    case GetFieldJson(_, fieldName) =>
      val commander = sender()
      val field = schema.getField(fieldName)
      if (field != null) {
        commander ! encoderDecoder.jsonEncode(record.get(field.pos), field.schema)
      } else {
        val ex = new RuntimeException("Field does not exist: " + fieldName)
        log.error(ex, ex.getMessage)
        commander ! Failure(ex)
      }

    case PutRecord(_, rec) =>
      commitRecord(id, rec, sender(), doLimitSize = true)

    case PutRecordJson(_, json) =>
      val commander = sender()
      avro.jsonDecode(json, schema) match {
        case Success(rec: Record) =>
          commitRecord(id, rec, commander, doLimitSize = true)
        case Success(_) =>
          val ex = new RuntimeException("Json could not to be parsed to a record: " + json)
          log.error(ex, ex.getMessage)
          commander ! Failure(ex)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case PutField(_, fieldName, value) =>
      val commander = sender()
      val field = schema.getField(fieldName)
      if (field != null) {
        commitField(id, value, field, commander, doLimitSize = true)
      } else {
        val ex = new RuntimeException("Field does not exist: " + fieldName)
        log.error(ex, ex.getMessage)
        commander ! Failure(ex)
      }

    case PutFieldJson(_, fieldName, json) =>
      val commander = sender()
      val field = schema.getField(fieldName)
      if (field != null) {
        avro.jsonDecode(json, field.schema) match {
          case Success(value) =>
            commitField(id, value, field, commander, doLimitSize = true)
          case x @ Failure(ex) =>
            log.error(ex, ex.getMessage)
            commander ! x
        }
      } else {
        val ex = new RuntimeException("Field does not exist: " + fieldName)
        log.error(ex, ex.getMessage)
        commander ! Failure(ex)
      }

    case Select(_, path) =>
      val commander = sender()
      avpath.select(parser)(record, path) match {
        case x @ Success(_) =>
          commander ! x
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case SelectAvro(_, path) =>
      val commander = sender()
      avpath.select(parser)(record, path) match {
        case x @ Success(ctxs) =>
          commander ! x // TODO encode to avro
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case SelectJson(_, path) =>
      val commander = sender()
      avpath.select(parser)(record, path) match {
        case Success(ctxs) =>
          Try(ctxs.map {
            case Ctx(value, schema, _, _) => encoderDecoder.jsonEncode(value, schema).get
          }) match {
            case Success(xs) =>
              val json = xs.mkString("[", ",", "]")
              commander ! Success(json)
            case x @ Failure(ex) =>
              log.error(ex, ex.getMessage)
              commander ! x
          }
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case Update(_, path, value) =>
      val commander = sender()
      val toBe = new GenericData.Record(record, true)
      avpath.update(parser)(toBe, path, value) match {
        case Success(ctxs) =>
          commit(id, toBe, ctxs, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case UpdateJson(_, path, value) =>
      val commander = sender()
      val toBe = new GenericData.Record(record, true)
      avpath.updateJson(parser)(toBe, path, value) match {
        case Success(ctxs) =>
          commit(id, toBe, ctxs, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case Insert(_, path, value) =>
      val commander = sender()
      val toBe = new GenericData.Record(record, true)
      avpath.insert(parser)(toBe, path, value) match {
        case Success(ctxs) =>
          commit(id, toBe, ctxs, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case InsertJson(_, path, value) =>
      val commander = sender()
      val toBe = new GenericData.Record(record, true)
      avpath.insertJson(parser)(toBe, path, value) match {
        case Success(ctxs) =>
          commit(id, toBe, ctxs, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case InsertAll(_, path, values) =>
      val commander = sender()
      val toBe = new GenericData.Record(record, true)
      avpath.insertAll(parser)(toBe, path, values) match {
        case Success(ctxs) =>
          commit(id, toBe, ctxs, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case InsertAllJson(_, path, values) =>
      val commander = sender()
      val toBe = new GenericData.Record(record, true)
      avpath.insertAllJson(parser)(toBe, path, values) match {
        case Success(ctxs) =>
          commit(id, toBe, ctxs, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case Delete(_, path) =>
      val commander = sender()
      val toBe = new GenericData.Record(record, true)
      avpath.delete(parser)(toBe, path) match {
        case Success(ctxs) =>
          commit(id, toBe, ctxs, commander, false)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case Clear(_, path) =>
      val commander = sender()
      val toBe = new GenericData.Record(record, true)
      avpath.clear(parser)(toBe, path) match {
        case Success(ctxs) =>
          commit(id, toBe, ctxs, commander, false)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case ReceiveTimeout =>
      log.info("Account got ReceiveTimeout")
    //context.parent ! Passivate(stopMessage = PoisonPill)
  }

  def persisting: Receive = {
    case Success(_) =>
      log.debug("Account persistence success: {}", id)
      context.become(ready)
      unstashAll()

    case PersistenceFailure(payload, sequenceNr, cause) =>
      log.error(cause, "")
      context.become(ready)
      unstashAll()

    case x =>
      log.debug("Entity got {}", x)
      stash()
  }

  private def commitRecord(id: String, toBe: Record, commander: ActorRef, doLimitSize: Boolean) {
    val fields = schema.getFields.iterator
    var updatedFields = List[(Schema.Field, Any)]()
    while (fields.hasNext) {
      val field = fields.next
      if (doLimitSize) {
        avro.toLimitedSize(toBe, field, limitedSize) match {
          case Some(newValue) => updatedFields ::= (field, newValue)
          case None           => updatedFields ::= (field, toBe.get(field.pos))
        }
      } else {
        updatedFields ::= (field, toBe.get(field.pos))
      }
    }
    commit2(id, updatedFields, commander)
  }

  private def commitField(id: String, value: Any, field: Schema.Field, commander: ActorRef, doLimitSize: Boolean) {
    val fields = schema.getFields.iterator
    //TODO if (doLimitSize) avro.limitToSize(rec, field, limitedSize)
    var updatedFields = List((field, value))
    commit2(id, updatedFields, commander)
  }

  private def commit(id: String, toBe: Record, ctxs: List[Evaluator.Ctx], commander: ActorRef, doLimitSize: Boolean = true) {
    val time = System.currentTimeMillis
    // TODO when avpath is ".", topLevelField will be null, it's better to return all topLevelFields
    val updatedFields =
      for (Evaluator.Ctx(value, schema, topLevelField, _) <- ctxs if topLevelField != null) yield {
        if (doLimitSize) {
          avro.toLimitedSize(toBe, topLevelField, limitedSize) match {
            case Some(newValue) => (topLevelField, newValue)
            case None           => (topLevelField, toBe.get(topLevelField.pos))
          }
        } else {
          (topLevelField, toBe.get(topLevelField.pos))
        }
      }

    if (updatedFields.isEmpty) {
      commitRecord(id, toBe, commander, doLimitSize = false)
    } else {
      commit2(id, updatedFields, commander)
    }
  }

  private def commit2(id: String, updatedFields: List[(Schema.Field, Any)], commander: ActorRef) {
    //context.become(persistingBehavior)
    persist(id, updatedFields).onComplete {
      case Success(_) =>
        val data = GenericData.get
        val size = updatedFields.size
        val fieldsBefore = Array.ofDim[(Schema.Field, Any)](size)
        var i = 0
        var fields = updatedFields
        while (i < size) {
          val (field, value) = fields.head
          fieldsBefore(i) = (field, data.deepCopy(field.schema, record.get(field.pos)))
          record.put(field.pos, value)
          fields = fields.tail
          i += 1
        }

        commander ! Success(id)
        self ! Success(id)
        self ! Entity.OnUpdated(id, fieldsBefore, record)

      case Failure(ex) =>
        commander ! Failure(ex)
        self ! PersistenceFailure(null, 0, ex) // TODO
    }
  }

  def persist(id: String, updatedFields: List[(Schema.Field, Any)]): Future[Unit] = Future.successful(())

}