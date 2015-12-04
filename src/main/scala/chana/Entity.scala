package chana

import akka.actor.{ Actor, ActorRef, ActorSystem, Cancellable, PoisonPill, Props, Stash }
import akka.contrib.pattern.{ ClusterSharding, ShardRegion, DistributedPubSubExtension }
import akka.event.LoggingAdapter
import akka.persistence._
import chana.avpath.Evaluator.Ctx
import chana.avro.DefaultRecordBuilder
import chana.serializer.AvroMarshaler
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.Record
import scala.concurrent.duration._
import scala.util.{ Failure, Success }

object Entity {
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

trait Entity extends Actor with Stash with PersistentActor {
  import context.dispatcher

  def log: LoggingAdapter

  def entityName: String
  def schema: Schema
  def builder: DefaultRecordBuilder

  def onUpdated(fieldsBefore: Array[(Schema.Field, Any)], recordAfter: Record) {}
  def onDeleted() {}

  def mediator = DistributedPubSubExtension(context.system).mediator
  lazy val avroMarshaler = new AvroMarshaler(schema)

  protected val id = self.path.name
  protected val encoderDecoder = new avro.EncoderDecoder()

  private var _isDeleted: Boolean = _
  protected def isDeleted = _isDeleted
  protected def isDeleted(b: Boolean) = {
    _isDeleted = b
    onDeleted()
  }

  val persistenceId: String = entityName + "_" + id

  protected var record: Record = _
  protected def loadRecord() = {
    builder.build()
  }

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
    case SnapshotOffer(metadata, offeredSnapshot: Array[Byte]) =>
      record = avroMarshaler.unmarshal(offeredSnapshot).asInstanceOf[Record]
    case x: SnapshotOffer       => log.warning("Recovery received unknown: {}", x)
    case x: UpdatedFields       => doUpdateRecord(x)
    case RecoveryFailure(cause) => log.error("Recovery failure: {}", cause)
    case RecoveryCompleted      => log.debug("Recovery completed: {}", id)
    case e: Event               =>
  }

  override def receiveCommand: Receive = accessBehavior orElse persistBehavior

  def persistBehavior: Receive = {
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
      commitRecord(id, rec, sender())

    case PutRecordJson(_, json) =>
      resetIdleTimeout()
      val commander = sender()
      avro.jsonDecode(json, schema) match {
        case Success(rec: Record) =>
          commitRecord(id, rec, commander)
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
      putField(sender(), fieldName, value)

    case PutFieldJson(_, fieldName, json) =>
      resetIdleTimeout()
      putFieldJson(sender(), fieldName, json)

    case Entity.SetIdleTimeout(milliseconds) =>
      setIdleTimeout(milliseconds.milliseconds)

    case Entity.IdleTimeout =>
      log.info("{}: {} idle timeout", entityName, id)
      cancelIdleTimeout()
      context.parent ! ShardRegion.Passivate(stopMessage = PoisonPill)
  }

  private def putField(commander: ActorRef, fieldName: String, value: Any) = {
    val field = schema.getField(fieldName)
    if (field != null) {
      commitField(id, value, field, commander)
    } else {
      val ex = new RuntimeException("Field does not exist: " + fieldName)
      log.error(ex, ex.getMessage)
      commander ! Failure(ex)
    }
  }

  private def putFieldJson(commander: ActorRef, fieldName: String, json: String) = {
    val commander = sender()
    val field = schema.getField(fieldName)
    if (field != null) {
      avro.jsonDecode(json, field.schema) match {
        case Success(value) =>
          commitField(id, value, field, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }
    } else {
      val ex = new RuntimeException("Field does not exist: " + fieldName)
      log.error(ex, ex.getMessage)
      commander ! Failure(ex)
    }
  }

  protected def putFields(commander: ActorRef, _fieldToValue: List[(String, Any)]) = {
    var fieldToValue = _fieldToValue
    var updatedFields = List[(Schema.Field, Any)]()
    var error: Option[Exception] = None
    while (fieldToValue.nonEmpty && error.isEmpty) {
      val (fieldName, value) = fieldToValue.head
      val field = schema.getField(fieldName)
      if (field != null) {
        updatedFields ::= (field, value)
      } else {
        val ex = new RuntimeException("Field does not exist: " + fieldName)
        log.error(ex, ex.getMessage)
        error = Some(ex)
      }
      fieldToValue = fieldToValue.tail
    }

    error match {
      case None     => commitUpdatedFields(id, updatedFields, commander)
      case Some(ex) => commander ! Failure(ex)
    }
  }

  private def commitRecord(id: String, toBe: Record, commander: ActorRef) {
    val fields = schema.getFields.iterator
    var updatedFields = List[(Schema.Field, Any)]()
    while (fields.hasNext) {
      val field = fields.next
      updatedFields ::= (field, toBe.get(field.pos))
    }
    commitUpdatedFields(id, updatedFields, commander)
  }

  private def commitField(id: String, value: Any, field: Schema.Field, commander: ActorRef) {
    val fields = schema.getFields.iterator
    var updatedFields = List((field, value))
    commitUpdatedFields(id, updatedFields, commander)
  }

  protected def commit(id: String, toBe: Record, ctxs: List[Ctx], commander: ActorRef) {
    val time = System.currentTimeMillis
    // TODO when avpath is ".", topLevelField will be null, it's better to return all topLevelFields
    val updatedFields =
      for (Ctx(value, schema, topLevelField, _) <- ctxs if topLevelField != null) yield {
        (topLevelField, toBe.get(topLevelField.pos))
      }

    if (updatedFields.isEmpty) {
      commitRecord(id, toBe, commander)
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

  private def commitUpdatedEvent(fieldsBefore: Array[(Schema.Field, Any)], commander: ActorRef)(event: UpdatedFields): Unit = {
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
    onUpdated(fieldsBefore, record)
  }

  private def doUpdateRecord(event: UpdatedFields): Unit = {
    event.updatedFields foreach { case (pos, value) => record.put(pos, value) }
  }

  /** for test only */
  def dummyPersist[A](event: A)(handler: A => Unit): Unit = handler(event)
}
