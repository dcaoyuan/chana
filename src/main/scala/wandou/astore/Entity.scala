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
    record = new GenericRecordBuilder(schema).build
  }

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
      sender() ! record
    case avpath.GetField(_, field) =>
      sender() ! record.get(field)
    case avpath.PutRecord(_, srcRecord) =>
      try {
        avro.replace(record, srcRecord)
        sender() ! Success(id)
      } catch {
        case ex: Throwable =>
          log.error(ex, ex.getMessage)
          sender() ! Failure(ex)
      }
    case avpath.PutRecordJson(_, json) =>
      try {
        val srcRecord = avro.FromJson.fromJsonString(json, schema).asInstanceOf[Record]
        avro.replace(record, srcRecord)
        sender() ! Success(id)
      } catch {
        case ex: Throwable =>
          log.error(ex, ex.getMessage)
          sender() ! Failure(ex)
      }
    case avpath.PutField(_, fieldName, value) =>
      try {
        val field = schema.getField(fieldName)
        if (field != null) {
          record.put(fieldName, value)
          sender() ! Success(id)
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
          record.put(fieldName, value)
          sender() ! Success(id)
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
      commit(rec, ctxs, self, sender())

    case avpath.UpdateJson(_, path, value) =>
      val rec = new GenericData.Record(record, true)
      val ast = avpathParser.parse(path)
      val ctxs = Evaluator.updateJson(rec, ast, value)
      commit(rec, ctxs, self, sender())

    case avpath.Insert(_, path, value) =>
      val rec = new GenericData.Record(record, true)
      val ast = avpathParser.parse(path)
      val ctxs = Evaluator.insert(rec, ast, value)
      commit(rec, ctxs, self, sender())

    case avpath.InsertJson(_, path, value) =>
      val rec = new GenericData.Record(record, true)
      val ast = avpathParser.parse(path)
      val ctxs = Evaluator.insertJson(rec, ast, value)
      commit(rec, ctxs, self, sender())

    case avpath.InsertAll(_, path, xs) =>
      val rec = new GenericData.Record(record, true)
      val ast = avpathParser.parse(path)
      val ctxs = Evaluator.insertAll(rec, ast, xs)
      commit(rec, ctxs, self, sender())

    case avpath.InsertAllJson(_, path, xs) =>
      val rec = new GenericData.Record(record, true)
      val ast = avpathParser.parse(path)
      val ctxs = Evaluator.insertAllJson(rec, ast, xs)
      commit(rec, ctxs, self, sender())

    case avpath.Delete(_, path) =>
      val rec = new GenericData.Record(record, true)
      val ast = avpathParser.parse(path)
      val ctxs = Evaluator.delete(rec, ast)
      commit(rec, ctxs, self, sender(), false)

    case avpath.Clear(_, path) =>
      val rec = new GenericData.Record(record, true)
      val ast = avpathParser.parse(path)
      val ctxs = Evaluator.clear(rec, ast)
      commit(rec, ctxs, self, sender(), false)

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

  private def commit(rec: Record, ctxs: List[Evaluator.Ctx], me: ActorRef, commander: ActorRef, doLimitSize: Boolean = true) {
    val time = System.currentTimeMillis
    val schema = rec.getSchema
    // TODO: when avpath is ".", topLevelField will be null, it's better to return all topLevelFields
    val modifiedFields =
      for (Evaluator.Ctx(value, schema, topLevelField, target) <- ctxs if topLevelField != null) yield {
        if (doLimitSize) avro.limitToSize(rec, topLevelField, 30)
        (topLevelField, rec.get(topLevelField.name))
      }

    //context.become(persisting)
    persist(id, modifiedFields).onComplete {
      case Success(_) =>
        if (modifiedFields.nonEmpty) {
          for ((field, value) <- modifiedFields) {
            record.put(field.name, value)
          }
        } else {
          avro.replace(record, rec)
        }

        commander ! Success(id)
        me ! Success(id)

      case Failure(ex) =>
        commander ! Failure(ex)
        me ! PersistenceFailure(null, 0, ex) // todo
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