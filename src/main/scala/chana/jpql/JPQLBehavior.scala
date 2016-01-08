package chana.jpql

import akka.contrib.pattern.DistributedPubSubMediator.{ Subscribe, SubscribeAck }
import chana.Entity
import chana.PutJPQL
import chana.avro.UpdateAction
import chana.avro.UpdateEvent
import chana.jpql
import org.apache.avro.generic.GenericData.Record
import scala.concurrent.duration._
import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.util.Failure
import scala.util.Success

object JPQLBehavior {
  val jpqlTopic = "chana_jpql_"

  private case class ReportingTick(key: String)
  private def reportingDelay(interval: Duration) = ThreadLocalRandom.current.nextLong(100, interval.toMillis).millis
}

/**
 * TODO pass entity id to jpql evaluator
 */
trait JPQLBehavior extends Entity {
  import JPQLBehavior._

  private var scheduledJpqls = Set[String]()
  private def isScheduled(jpqlKey: String) = scheduledJpqls.contains(jpqlKey)

  mediator ! Subscribe(jpqlTopic + entityName, self)

  import context.dispatcher
  // TODO: 
  // 1. report once when new-created - Done, see onReady()
  // 2  report when new jpql is put - Done, see behavior when got PutJPQL
  // 3. report only on updated  - Done, see onUpdated()
  // 4. report once when deleted  // in case of deleted, should guarantee via ACK etc?

  def jpqlBehavior: Receive = {

    case ReportingTick(jpqlKey) =>
      DistributedJPQLBoard.keyToJPQL.get(jpqlKey) match {
        case (meta: JPQLSelect, interval) if meta.entity == entityName =>
          scheduleJpqlReport(jpqlKey, meta, interval, record)
        case _ =>
      }

    case SubscribeAck(Subscribe(topic, None, `self`)) =>
      log.debug("Subscribed " + topic)

    case PutJPQL(_, key, jpqlQuery, interval) =>

      jpql.parseJPQL(key, jpqlQuery) match {

        case Success(meta: jpql.JPQLSelect) =>
          if (meta.entity == entityName) {
            scheduleJpqlReport(key, meta, interval, record)
          }

        case Success(meta: jpql.JPQLUpdate) =>
          val commander = sender()
          jpql.update(id, record, meta) match {
            case Success(actions) =>
              if (actions.nonEmpty) {
                resetIdleTimeout()
                commit(id, actions.flatten, commander)
              }
            case x @ Failure(ex) =>
              log.error(ex, ex.getMessage)
              commander ! x
          }

        case Success(meta: jpql.JPQLInsert) =>
          val commander = sender()
          jpql.insert(id, record, meta) match {
            case Success(actions) =>
              if (actions.nonEmpty) {
                resetIdleTimeout()
                commit(id, actions, commander)
              }
            case x @ Failure(ex) =>
              log.error(ex, ex.getMessage)
              commander ! x
          }

        case Success(meta: jpql.JPQLDelete) =>
          val commander = sender()
          jpql.delete(id, record, meta) match {
            case Success(actions) =>
              if (actions.collectFirst { case UpdateAction(null, _, binlog) => true }.isDefined) {
                isDeleted(true) // TODO persist log
              } else {
                commit(id, actions, commander)
              }
            case failure @ Failure(ex) =>
              log.error(ex, ex.getMessage)
              commander ! failure
          }

        case failure @ Failure(ex) =>
          val commander = sender()
          log.error(ex, ex.getMessage)
          commander ! failure
      }

  }

  /**
   * Schedule periodic reporting if any jpql select is not scheduled
   */
  private def scheduleJpqlReportAll(force: Boolean) {
    var newScheduledJpqls = Set[String]()
    val jpqls = DistributedJPQLBoard.keyToJPQL.entrySet.iterator

    while (jpqls.hasNext) {
      val entry = jpqls.next
      val key = entry.getKey
      val (meta, interval) = entry.getValue
      if (meta.entity == entityName) {
        if (force || !isScheduled(key)) {
          scheduleJpqlReport(key, meta.asInstanceOf[JPQLSelect], interval, record) // report right now 
        }
        newScheduledJpqls += key
      }
    }
    scheduledJpqls = newScheduledJpqls
  }

  private def scheduleJpqlReport(jpqlKey: String, meta: JPQLSelect, interval: FiniteDuration, record: Record) {
    if (meta.entity == entityName) {
      try {
        if (isDeleted) {
          val deleted = DeletedRecord(id)
          JPQLReducer.reducerProxy(context.system, jpqlKey) ! deleted
        } else {
          val projection = new JPQLMapperSelect(id, meta).gatherProjection(record)
          JPQLReducer.reducerProxy(context.system, jpqlKey) ! projection
        }
        context.system.scheduler.scheduleOnce(interval, self, ReportingTick(jpqlKey))

      } catch {
        case ex: Throwable => log.error(ex, ex.getMessage)
      }
    }
  }

  override def onReady() {
    scheduleJpqlReportAll(force = true)
  }

  override def onUpdated(event: UpdateEvent) {
    scheduleJpqlReportAll(true)
  }

}
