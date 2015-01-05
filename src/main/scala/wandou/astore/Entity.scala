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
import javax.script.SimpleBindings
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecordBuilder
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import wandou.astore.script.Scriptable
import wandou.avpath
import wandou.avpath.Evaluator
import wandou.avpath.Evaluator.Ctx
import wandou.avro

object Entity {
  def props(schema: Schema) = Props(classOf[Entity], schema)

  lazy val idExtractor: ShardRegion.IdExtractor = {
    case cmd: avpath.Command => (cmd.id, cmd)
  }

  lazy val shardResolver: ShardRegion.ShardResolver = {
    case cmd: avpath.Command => (cmd.id.hashCode % 100).toString
  }

  def startSharding(system: ActorSystem, shardName: String, entryProps: Option[Props]) = {
    ClusterSharding(system).start(
      typeName = shardName,
      entryProps = entryProps,
      idExtractor = idExtractor,
      shardResolver = shardResolver)
  }

  private[astore] object Internal {
    final case class Bootstrap(val record: Record)
  }
}

class Entity(schema: Schema) extends Actor with Stash with ActorLogging with Scriptable {
  import context.dispatcher

  private val id = self.path.name
  private val avpathParser = new avpath.Parser()

  private var record: Record = _
  private def createRecord() {
    record = new GenericRecordBuilder(schema).build()
  }
  private var limitedSize = 30

  override def preStart {
    super[Actor].preStart
    log.debug("Starting: {} ", id)
    //context.setReceiveTimeout(ReceiveTimeoutSecs)
    createRecord()
    // TODO load persistented data
    self ! Entity.Internal.Bootstrap(record)
  }

  override def receive: Receive = initial

  def initial: Receive = {
    case Entity.Internal.Bootstrap(r) =>
      record = r
      context.become(ready orElse scriptableBehavior)
      unstashAll()
    case _ =>
      stash()
  }

  def ready: Receive = {
    case avpath.GetRecord(_) =>
      sender() ! Ctx(record, schema, null)

    case avpath.GetField(_, fieldName) =>
      val field = schema.getField(fieldName)
      if (field != null) {
        sender() ! Ctx(record.get(field.pos), field.schema, field)
      } else {
        // send what ? TODO
      }
    case avpath.PutRecord(_, rec) =>
      commitRecord(rec, sender(), doLimitSize = true)

    case avpath.PutRecordJson(_, json) =>
      try {
        val rec = avro.FromJson.fromJsonString(json, schema).asInstanceOf[Record]
        commitRecord(rec, sender(), doLimitSize = true)
      } catch {
        case ex: Throwable =>
          log.error(ex, ex.getMessage)
          sender() ! Failure(ex)
      }

    case avpath.PutField(_, fieldName, value) =>
      try {
        val field = schema.getField(fieldName)
        if (field != null) {
          commitField(value, field, sender(), doLimitSize = true)
        } else {
          sender() ! Failure(new RuntimeException("Field does not exist: " + fieldName))
        }
      } catch {
        case ex: Throwable =>
          sender() ! Failure(ex)
      }

    case avpath.PutFieldJson(_, fieldName, json) =>
      try {
        val field = schema.getField(fieldName)
        if (field != null) {
          val value = avro.FromJson.fromJsonString(json, field.schema)
          commitField(value, field, sender(), doLimitSize = true)
        } else {
          sender() ! Failure(new RuntimeException("Field does not exist: " + fieldName))
        }
      } catch {
        case ex: Throwable =>
          sender() ! Failure(ex)
      }

    case avpath.Select(_, path) =>
      val ast = avpathParser.parse(path)
      val res = Evaluator.select(record, ast)
      sender() ! res

    case avpath.Update(_, path, value) =>
      val rec = new GenericData.Record(record, true)
      val ast = avpathParser.parse(path)
      val ctxs = Evaluator.update(rec, ast, value)
      commit(rec, ctxs, sender())

    case avpath.UpdateJson(_, path, value) =>
      val rec = new GenericData.Record(record, true)
      val ast = avpathParser.parse(path)
      val ctxs = Evaluator.updateJson(rec, ast, value)
      commit(rec, ctxs, sender())

    case avpath.Insert(_, path, value) =>
      val rec = new GenericData.Record(record, true)
      val ast = avpathParser.parse(path)
      val ctxs = Evaluator.insert(rec, ast, value)
      commit(rec, ctxs, sender())

    case avpath.InsertJson(_, path, value) =>
      val rec = new GenericData.Record(record, true)
      val ast = avpathParser.parse(path)
      val ctxs = Evaluator.insertJson(rec, ast, value)
      commit(rec, ctxs, sender())

    case avpath.InsertAll(_, path, xs) =>
      val rec = new GenericData.Record(record, true)
      val ast = avpathParser.parse(path)
      val ctxs = Evaluator.insertAll(rec, ast, xs)
      commit(rec, ctxs, sender())

    case avpath.InsertAllJson(_, path, xs) =>
      val rec = new GenericData.Record(record, true)
      val ast = avpathParser.parse(path)
      val ctxs = Evaluator.insertAllJson(rec, ast, xs)
      commit(rec, ctxs, sender())

    case avpath.Delete(_, path) =>
      val rec = new GenericData.Record(record, true)
      val ast = avpathParser.parse(path)
      val ctxs = Evaluator.delete(rec, ast)
      commit(rec, ctxs, sender(), false)

    case avpath.Clear(_, path) =>
      val rec = new GenericData.Record(record, true)
      val ast = avpathParser.parse(path)
      val ctxs = Evaluator.clear(rec, ast)
      commit(rec, ctxs, sender(), false)

    case ReceiveTimeout =>
      log.info("Account got ReceiveTimeout")
    //context.parent ! Passivate(stopMessage = PoisonPill)
  }

  def persisting: Receive = {
    case Success(_) =>
      log.debug("Account persistence success: {}", id)
      context.become(ready orElse scriptableBehavior)
      unstashAll()

    case PersistenceFailure(payload, sequenceNr, cause) =>
      log.error(cause, "")
      context.become(ready orElse scriptableBehavior)
      unstashAll()

    case x =>
      log.debug("Entity got {}", x)
      stash()
  }

  private def commitRecord(rec: Record, commander: ActorRef, doLimitSize: Boolean) {
    val fields = schema.getFields.iterator
    var updatedFields = List[(Schema.Field, Any)]()
    while (fields.hasNext) {
      val field = fields.next
      if (doLimitSize) avro.limitToSize(rec, field, limitedSize)
      updatedFields ::= (field, rec.get(field.pos))
    }
    commit2(id, updatedFields, commander)
  }

  private def commitField(value: Any, field: Schema.Field, commander: ActorRef, doLimitSize: Boolean) {
    val fields = schema.getFields.iterator
    //TODO if (doLimitSize) avro.limitToSize(rec, field, limitedSize)
    var updatedFields = List((field, value))
    commit2(id, updatedFields, commander)
  }

  private def commit(rec: Record, ctxs: List[Evaluator.Ctx], commander: ActorRef, doLimitSize: Boolean = true) {
    val time = System.currentTimeMillis
    // TODO when avpath is ".", topLevelField will be null, it's better to return all topLevelFields
    val updatedFields =
      for (Evaluator.Ctx(value, schema, topLevelField, _) <- ctxs if topLevelField != null) yield {
        if (doLimitSize) avro.limitToSize(rec, topLevelField, limitedSize)
        (topLevelField, rec.get(topLevelField.pos))
      }

    if (updatedFields.isEmpty) {
      commitRecord(rec, commander, doLimitSize = false)
    } else {
      commit2(id, updatedFields, commander)
    }
  }

  private def commit2(id: String, modifiedFields: List[(Schema.Field, Any)], commander: ActorRef) {
    //context.become(persisting)
    persist(id, modifiedFields).onComplete {
      case Success(_) =>
        for ((field, value) <- modifiedFields) {
          record.put(field.pos, value)
        }
        commander ! Success(id)
        self ! Success(id)

      case Failure(ex) =>
        commander ! Failure(ex)
        self ! PersistenceFailure(null, 0, ex) // TODO
    }
  }

  def persist(id: String, modifiedFields: List[(Schema.Field, Any)]): Future[Unit] = Future.successful(())

  // ---- implementation of Scriptable
  def prepareBindings = {
    val bindings = new SimpleBindings
    bindings.put("id", id)
    bindings.put("record", record)
    bindings
  }
}