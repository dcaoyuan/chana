package chana

import akka.actor.{ Actor, ActorRef, ActorSystem, Cancellable, PoisonPill, Props, Stash }
import akka.contrib.pattern.{ ClusterSharding, ShardRegion, DistributedPubSubExtension }
import akka.event.LoggingAdapter
import akka.persistence._
import chana.avpath.Evaluator.Ctx
import chana.avro.Binlog
import chana.avro.Changelog
import chana.avro.DefaultRecordBuilder
import chana.avro.UpdateAction
import chana.avro.UpdateEvent
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.Record
import scala.concurrent.duration._
import scala.util.{ Failure, Success }

object Entity {
  val idExtractor: ShardRegion.IdExtractor = {
    case cmd: Command => (cmd.id, cmd)
  }

  val shardResolver: ShardRegion.ShardResolver = {
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

  def onUpdated(event: UpdateEvent) {}
  def onDeleted() {}

  def mediator = DistributedPubSubExtension(context.system).mediator

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
      record = avro.avroDecode[Record](offeredSnapshot, schema).get
    case x: Binlog =>
      doUpdateRecord(x)
    case x: SnapshotOffer       => log.warning("Recovery received unknown: {}", x)
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
          val ex = new RuntimeException("JSON could not to be parsed to a record: " + json)
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

  private def putFields(commander: ActorRef, _fieldToValue: List[(String, Any)]) = {
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
      case None     => commitFields(id, updatedFields, commander)
      case Some(ex) => commander ! Failure(ex)
    }
  }

  private def commitRecord(id: String, toBe: Record, commander: ActorRef) {
    var actions = List[UpdateAction]()

    val fields = schema.getFields.iterator
    while (fields.hasNext) {
      val field = fields.next
      val value = toBe.get(field.pos)

      val prev = record.get(field.pos)
      val rlback = { () => record.put(field.pos, prev) }
      val commit = { () => record.put(field.pos, value) }
      val xpath = "/" + field.name
      val bytes = avro.avroEncode(value, field.schema).get
      actions ::= UpdateAction(commit, rlback, Changelog(xpath, value, bytes))
    }

    commit(id, actions.reverse, commander)
  }

  private def commitField(id: String, value: Any, field: Schema.Field, commander: ActorRef) {
    commitFields(id, List((field, value)), commander)
  }

  private def commitFields(id: String, updateFields: List[(Schema.Field, Any)], commander: ActorRef) {
    var actions = List[UpdateAction]()

    for { (field, value) <- updateFields } {
      val prev = record.get(field.pos)
      val rlback = { () => record.put(field.pos, prev) }
      val commit = { () => record.put(field.pos, value) }
      val xpath = "/" + field.name
      val bytes = avro.avroEncode(value, field.schema).get
      actions ::= UpdateAction(commit, rlback, Changelog(xpath, value, bytes))
    }

    commit(id, actions.reverse, commander)
  }

  protected def commit(id: String, actions: List[UpdateAction], commander: ActorRef) {
    val event = UpdateEvent(actions.map(_.binlog).toArray)

    // TODO configuration options to persistAsync etc.
    if (isPersistent) {
      persist(event)(internal_commit(actions, commander))
    } else {
      internal_commit(actions, commander)(event)
    }
  }

  private def internal_commit(actions: List[UpdateAction], commander: ActorRef)(event: UpdateEvent) {
    actions foreach { _.commit() }

    if (persistCount >= persistParams) {
      if (isPersistent) {
        saveSnapshot(avro.avroEncode(record, schema).get)
      }
      // if saveSnapshot failed, we don't care about it, since we've got 
      // events persisted. Anyway, we'll try saveSnapshot at next round
      persistCount = 0
    } else {
      persistCount += 1
    }

    commander ! Success(id)
    onUpdated(event)
  }

  private def doUpdateRecord(event: UpdatedFields): Unit = {
    event.updatedFields foreach { case (pos, value) => record.put(pos, value) }
  }

  private def doUpdateRecord(binlog: Binlog): Unit = {
    // TODO
  }

  /** for test only */
  def dummyPersist[A](event: A)(handler: A => Unit): Unit = handler(event)
}
