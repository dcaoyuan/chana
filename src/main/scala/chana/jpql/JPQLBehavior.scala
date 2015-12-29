package chana.jpql

import akka.contrib.pattern.DistributedPubSubMediator.{ Subscribe, SubscribeAck }
import chana.Entity
import chana.avro.UpdateAction
import chana.avro.UpdateEvent
import chana.jpql
import org.apache.avro.generic.GenericData.Record
import scala.concurrent.duration._
import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.util.Failure
import scala.util.Success

object JPQLBehavior {
  final case class ReportingTick(key: String)

  private def reportingDelay(interval: Duration) = ThreadLocalRandom.current.nextLong(100, interval.toMillis).millis

  val JPQLTopic = "jpql_topic"
}

trait JPQLBehavior extends Entity {
  import JPQLBehavior._

  private var reportingJpqls = List[String]()

  mediator ! Subscribe(JPQLTopic, self)

  import context.dispatcher
  // TODO: 
  // 1. report once when new-created/new-jpql 
  // 2. report once when deleted  // in case of deleted, should guarantee via ACK etc?
  // 3. report only on updated 
  context.system.scheduler.schedule(1.seconds, 1.seconds, self, ReportingTick(""))

  def jpqlBehavior: Receive = {
    case ReportingTick("") => reportAll(false)
    case ReportingTick(jpqlKey) =>
      DistributedJPQLBoard.keyToStatement.get(jpqlKey) match {
        case (meta: JPQLSelect, interval) => report(jpqlKey, meta, interval, record)
        case _                            =>
      }

    case SubscribeAck(Subscribe(JPQLTopic, None, `self`)) =>
      log.debug("Subscribed " + JPQLTopic)

    case jpqlMeta @ JPQLUpdate(stmt, asToEntity, asToJoin) =>
      resetIdleTimeout()
      val commander = sender()
      jpql.update(record, jpqlMeta) match {
        case Success(actions) =>
          actions.flatten
          commit(id, actions.flatten, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case jpqlMeta @ JPQLInsert(stmt, asToEntity, asToJoin) =>
      resetIdleTimeout()
      val commander = sender()
      jpql.insert(record, jpqlMeta) match {
        case Success(actions) =>
          commit(id, actions, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case jpqlMeta @ JPQLDelete(stmt, asToEntity, asToJoin) =>
      resetIdleTimeout()
      val commander = sender()
      jpql.delete(record, jpqlMeta) match {
        case Success(actions) =>
          if (actions.collectFirst { case UpdateAction(null, _, binlog) => true }.isDefined) {
            isDeleted(true) // TODO persist log
          } else {
            commit(id, actions, commander)
          }
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

  }

  def reportAll(force: Boolean) {
    var newReportingJpqls = List[String]()
    val jpqls = DistributedJPQLBoard.keyToStatement.entrySet.iterator

    while (jpqls.hasNext) {
      val entry = jpqls.next
      val key = entry.getKey
      if (force || !reportingJpqls.contains(key)) {
        val (meta, interval) = entry.getValue
        report(key, meta.asInstanceOf[JPQLSelect], interval, record) // report at once
      }
      newReportingJpqls ::= key
    }
    reportingJpqls = newReportingJpqls
  }

  def report(jpqlKey: String, meta: JPQLSelect, interval: FiniteDuration, record: Record) {
    try {
      if (isDeleted) {
        val deleted = DeletedRecord(id)
        JPQLReducer.reducerProxy(context.system, jpqlKey) ! deleted
      } else {
        val projection = new JPQLMapperSelect(meta).gatherProjection(id, record)
        JPQLReducer.reducerProxy(context.system, jpqlKey) ! projection
      }

      context.system.scheduler.scheduleOnce(interval, self, ReportingTick(jpqlKey))
    } catch {
      case ex: Throwable => log.error(ex, ex.getMessage)
    }
  }

  override def onUpdated(event: UpdateEvent) {
    reportAll(true)
  }

}
