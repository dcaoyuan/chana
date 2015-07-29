package chana

import akka.actor.{ Actor, ActorLogging, ActorRef, ActorSystem, Cancellable, PoisonPill, Props, Stash }
import akka.contrib.pattern.{ ClusterSharding, ShardRegion }
import akka.event.LoggingAdapter
import akka.persistence._
import chana.avpath.Evaluator.Ctx
import chana.avro.RecordBuilder
import chana.jpql.JPQLReporting
import chana.script.Scriptable
import chana.serializer.AvroMarshaler
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.Record
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

object Entity {
  def props(entityName: String, schema: Schema, builder: RecordBuilder, idleTimeout: Duration) =
    Props(classOf[AEntity], entityName, schema, builder, idleTimeout)

  lazy val idExtractor: ShardRegion.IdExtractor = {
    case cmd: Command => (cmd.id, cmd)
  }

  lazy val shardResolver: ShardRegion.ShardResolver = {
    case cmd: Command => (cmd.id.hashCode % 100).toString
  }

  def startSharding(system: ActorSystem, shardName: String, entryProps: Option[Props]) =
    ClusterSharding(system).start(
      typeName = shardName,
      entryProps = entryProps,
      idExtractor = idExtractor,
      shardResolver = shardResolver)

  private final val emptyCancellable: Cancellable = new Cancellable {
    def isCancelled: Boolean = false
    def cancel(): Boolean = false
  }
  private final val emptyIdleTimeoutData: (Duration, Cancellable) = (Duration.Undefined, emptyCancellable)

  case object IdleTimeout
  final case class SetIdleTimeout(milliseconds: Long)

  final case class Bootstrap(record: Record)
}

class AEntity(val entityName: String, val schema: Schema, val builder: RecordBuilder, idleTimeout: Duration)
    extends Entity
    with Scriptable
    with JPQLReporting
    with Actor
    with ActorLogging {

  idleTimeout match {
    case f: FiniteDuration =>
      setIdleTimeout(f)
      resetIdleTimeout()
    case _ =>
  }

  override def receiveCommand: Receive = super.receiveCommand orElse {
    case JPQLReporting.Reporting => onQuery(id, record)
  }
}

trait Entity extends Actor with Stash with PersistentActor {
  import context.dispatcher

  def log: LoggingAdapter

  def entityName: String
  def schema: Schema
  def builder: RecordBuilder
  def onUpdated(id: String, fieldsBefore: Array[(Schema.Field, Any)], recordAfter: Record)

  lazy val avroMarshaler = new AvroMarshaler(schema)

  protected val id = self.path.name
  protected val encoderDecoder = new avro.EncoderDecoder()
  protected def parser = new avpath.Parser()

  val persistenceId: String = entityName + "_" + id

  protected var record: Record = _
  protected def loadRecord() = {
    builder.build()
  }

  protected var limitedSize = Int.MaxValue // TODO
  protected val isPersistent = context.system.settings.config.getBoolean("chana.persistence.persistent")
  protected var persistParams = context.system.settings.config.getInt("chana.persistence.nrOfEventsBetweenSnapshots")
  protected var persistCount = 0

  private var idleTimeoutData: (Duration, Cancellable) = Entity.emptyIdleTimeoutData
  final def idleTimeout: Duration = idleTimeoutData._1
  final def setIdleTimeout(timeout: Duration): Unit = idleTimeoutData = idleTimeoutData.copy(_1 = timeout)
  final def resetIdleTimeout() {
    val idletimeout = idleTimeoutData
    idletimeout._1 match {
      case f: FiniteDuration =>
        idletimeout._2.cancel() // Cancel any ongoing future
        val task = context.system.scheduler.scheduleOnce(f, self, Entity.IdleTimeout)
        idleTimeoutData = (f, task)
      case _ => cancelIdleTimeout()
    }
  }
  final def cancelIdleTimeout() {
    if (idleTimeoutData._2 ne Entity.emptyCancellable) {
      idleTimeoutData._2.cancel()
      idleTimeoutData = (idleTimeoutData._1, Entity.emptyCancellable)
    }
  }

  override def preStart {
    super[Actor].preStart
    log.debug("Starting: {} ", id)
    record = loadRecord()
    self ! Recover()
  }

  override def receiveRecover: Receive = {
    case SnapshotOffer(metadata, offeredSnapshot: Array[Byte]) => record = avroMarshaler.unmarshal(offeredSnapshot).asInstanceOf[Record]
    case x: SnapshotOffer => log.warning("Recovery received unknown: {}", x)
    case event: Event => doUpdateRecord(event)
    case RecoveryFailure(cause) => log.error("Recovery failure: {}", cause)
    case RecoveryCompleted => log.debug("Recovery completed: {}", id)
  }

  override def receiveCommand: Receive = accessBehavior orElse {
    case f: PersistenceFailure  => log.error("persist failed: {}", f.cause)
    case f: SaveSnapshotFailure => log.error("saving snapshot failed: {}", f.cause)
  }

  def accessBehavior: Receive = {
    case GetRecord(_) =>
      resetIdleTimeout()
      sender() ! Success(Ctx(new Record(record, true), schema, null))

    case GetRecordAvro(_) =>
      resetIdleTimeout()
      sender() ! encoderDecoder.avroEncode(record, schema)

    case GetRecordJson(_) =>
      resetIdleTimeout()
      sender() ! encoderDecoder.jsonEncode(record, schema)

    case GetField(_, fieldName) =>
      resetIdleTimeout()
      val commander = sender()
      val field = schema.getField(fieldName)
      if (field != null) {
        commander ! Success(Ctx(GenericData.get().deepCopy(field.schema(), record.get(field.pos)), field.schema, field))
      } else {
        val ex = new RuntimeException("Field does not exist: " + fieldName)
        log.error(ex, ex.getMessage)
        commander ! Failure(ex)
      }

    case GetFieldAvro(_, fieldName) =>
      resetIdleTimeout()
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
      resetIdleTimeout()
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
      resetIdleTimeout()
      commitRecord(id, rec, sender(), doLimitSize = true)

    case PutRecordJson(_, json) =>
      resetIdleTimeout()
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
      resetIdleTimeout()
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
      resetIdleTimeout()
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
      resetIdleTimeout()
      val commander = sender()
      avpath.select(parser)(record, path) match {
        case Success(ctxList: List[Ctx]) =>
          commander ! Success(ctxList.map { ctx =>
            Ctx(GenericData.get().deepCopy(ctx.schema, ctx.value), ctx.schema, ctx.topLevelField, ctx.target)
          })
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case SelectAvro(_, path) =>
      resetIdleTimeout()
      val commander = sender()
      avpath.select(parser)(record, path) match {
        case x @ Success(ctxs) =>
          Try(ctxs.map { ctx => encoderDecoder.avroEncode(ctx.value, ctx.schema).get }) match {
            case xs: Success[_] =>
              commander ! xs // List[Array[Byte]] 
            case x @ Failure(ex) =>
              log.error(ex, ex.getMessage)
              commander ! x
          }
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case SelectJson(_, path) =>
      resetIdleTimeout()
      val commander = sender()
      avpath.select(parser)(record, path) match {
        case Success(ctxs) =>
          Try(ctxs.map { ctx => encoderDecoder.jsonEncode(ctx.value, ctx.schema).get }) match {
            case xs: Success[_] =>
              commander ! xs // List[Array[Byte]]
            case x @ Failure(ex) =>
              log.error(ex, ex.getMessage)
              commander ! x
          }
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case Update(_, path, value) =>
      resetIdleTimeout()
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
      resetIdleTimeout()
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
      resetIdleTimeout()
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
      resetIdleTimeout()
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
      resetIdleTimeout()
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
      resetIdleTimeout()
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
      resetIdleTimeout()
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
      resetIdleTimeout()
      val commander = sender()
      val toBe = new GenericData.Record(record, true)
      avpath.clear(parser)(toBe, path) match {
        case Success(ctxs) =>
          commit(id, toBe, ctxs, commander, false)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case Entity.SetIdleTimeout(milliseconds) =>
      setIdleTimeout(milliseconds.milliseconds)

    case Entity.IdleTimeout =>
      log.info("{}: {} idle timeout", entityName, id)
      cancelIdleTimeout()
      context.parent ! ShardRegion.Passivate(stopMessage = PoisonPill)
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
    commitUpdatedFields(id, updatedFields, commander)
  }

  private def commitField(id: String, value: Any, field: Schema.Field, commander: ActorRef, doLimitSize: Boolean) {
    val fields = schema.getFields.iterator
    //TODO if (doLimitSize) avro.limitToSize(rec, field, limitedSize)
    var updatedFields = List((field, value))
    commitUpdatedFields(id, updatedFields, commander)
  }

  private def commit(id: String, toBe: Record, ctxs: List[Ctx], commander: ActorRef, doLimitSize: Boolean = true) {
    val time = System.currentTimeMillis
    // TODO when avpath is ".", topLevelField will be null, it's better to return all topLevelFields
    val updatedFields =
      for (Ctx(value, schema, topLevelField, _) <- ctxs if topLevelField != null) yield {
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
      commitUpdatedFields(id, updatedFields, commander)
    }
  }

  private def commitUpdatedFields(id: String, updatedFields: List[(Schema.Field, Any)], commander: ActorRef) {
    val data = GenericData.get
    val size = updatedFields.size
    val fieldsBefore = Array.ofDim[(Schema.Field, Any)](size)
    var i = 0
    var fields = updatedFields
    while (i < size) {
      val (field, value) = fields.head
      fieldsBefore(i) = (field, data.deepCopy(field.schema, record.get(field.pos)))
      fields = fields.tail
      i += 1
    }
    val event = UpdatedFields(updatedFields map { case (field, value) => (field.pos, value) })

    // TODO options to persistAsync etc.
    if (isPersistent) {
      persist(event)(commitUpdatedEvent(fieldsBefore, commander))
    } else {
      commitUpdatedEvent(fieldsBefore, commander)(event)
    }
  }

  private def commitUpdatedEvent(fieldsBefore: Array[(Schema.Field, Any)], commander: ActorRef)(event: Event): Unit = {
    doUpdateRecord(event)
    if (persistCount >= persistParams) {
      if (isPersistent) {
        saveSnapshot(avroMarshaler.marshal(record))
      }
      // if saveSnapshot failed, we don't care about it, since we've got 
      // events persisted. Anyway, we'll try saveSnapshot at next round
      persistCount = 0
    } else {
      persistCount += 1
    }

    commander ! Success(id)
    onUpdated(id, fieldsBefore, record)
  }

  private def doUpdateRecord(event: Event): Unit =
    event match {
      case UpdatedFields(updatedFields) =>
        updatedFields foreach { case (pos, value) => record.put(pos, value) }
    }

  /** for test only */
  def dummyPersist[A](event: A)(handler: A => Unit): Unit = handler(event)
}
