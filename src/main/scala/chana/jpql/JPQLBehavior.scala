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
  private val ReportingAllTick = ReportingTick("")
  private def reportingDelay(interval: Duration) = ThreadLocalRandom.current.nextLong(100, interval.toMillis).millis
}

/**
 * TODO pass entity id to jpql evaluator
 */
trait JPQLBehavior extends Entity {
  import JPQLBehavior._

  private var reportedJpqls = List[String]()

  mediator ! Subscribe(jpqlTopic + entityName, self)

  import context.dispatcher
  // TODO: 
  // 1. report once when new-created/new-jpql - Done see onReady()
  // 2. report once when deleted  // in case of deleted, should guarantee via ACK etc?
  // 3. report only on updated 
  context.system.scheduler.schedule(1.seconds, 1.seconds, self, ReportingAllTick)

  def jpqlBehavior: Receive = {
    case ReportingAllTick =>
      jpqlReportAll(false)

    case ReportingTick(jpqlKey) =>
      DistributedJPQLBoard.keyToStatement.get(jpqlKey) match {
        case (meta: JPQLSelect, interval) => jpqlReport(jpqlKey, meta, interval, record)
        case _                            =>
      }

    case SubscribeAck(Subscribe(topic, None, `self`)) =>
      log.debug("Subscribed " + topic)

    case PutJPQL(_, key, jpqlQuery, interval) =>

      jpql.parseJPQL(key, jpqlQuery) match {

        case Success(meta: jpql.JPQLSelect) =>
          resetIdleTimeout()
          jpqlReport(key, meta, interval, record)

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

  def jpqlReportAll(force: Boolean) {
    var newReportedJpqls = List[String]()
    val jpqls = DistributedJPQLBoard.keyToStatement.entrySet.iterator

    while (jpqls.hasNext) {
      val entry = jpqls.next
      val key = entry.getKey
      val (meta, interval) = entry.getValue
      if ((force || !reportedJpqls.contains(key)) && meta.entity == entityName) {
        jpqlReport(key, meta.asInstanceOf[JPQLSelect], interval, record) // report at once
      }
      newReportedJpqls ::= key
    }
    reportedJpqls = newReportedJpqls
  }

  def jpqlReport(jpqlKey: String, meta: JPQLSelect, interval: FiniteDuration, record: Record) {
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
    jpqlReportAll(force = true)
  }

  override def onUpdated(event: UpdateEvent) {
    jpqlReportAll(true)
  }

}
