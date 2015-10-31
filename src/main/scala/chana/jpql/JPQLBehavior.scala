package chana.jpql

import akka.contrib.pattern.DistributedPubSubMediator.{ Subscribe, SubscribeAck }
import chana.Entity
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import scala.concurrent.duration._
import scala.concurrent.forkjoin.ThreadLocalRandom

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
        case null             =>
        case (meta, interval) => report(jpqlKey, meta, interval, record)
      }

    case SubscribeAck(Subscribe(JPQLTopic, None, `self`)) =>
      log.info("Subscribed " + JPQLTopic)

    case JPQLDelete(stmt, projectionSchema, asToEntity, asToJoin) =>

  }

  def reportAll(force: Boolean) {
    var newReportingJpqls = List[String]()
    val jpqls = DistributedJPQLBoard.keyToStatement.entrySet.iterator
    while (jpqls.hasNext) {
      val entry = jpqls.next
      val key = entry.getKey
      if (force || !reportingJpqls.contains(key)) {
        val (meta, interval) = entry.getValue
        report(key, meta, interval, record) // report at once
      }
      newReportingJpqls ::= key
    }
    reportingJpqls = newReportingJpqls
  }

  def eval(meta: JPQLMeta, record: Record) = {
    val e = new JPQLMapperEvaluator(meta)
    e.gatherProjection(id, record)
  }

  def report(jpqlKey: String, meta: JPQLMeta, interval: FiniteDuration, record: Record) {
    try {
      val dataset = if (isDeleted) null else eval(meta, record)
      JPQLReducer.reducerProxy(context.system, jpqlKey) ! dataset
      context.system.scheduler.scheduleOnce(interval, self, ReportingTick(jpqlKey))
    } catch {
      case ex: Throwable => log.error(ex, ex.getMessage)
    }
  }

  override def onUpdated(fieldsBefore: Array[(Schema.Field, Any)], recordAfter: Record) {
    reportAll(true)
  }

}
