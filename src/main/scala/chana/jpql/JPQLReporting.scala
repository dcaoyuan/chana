package chana.jpql

import chana.Entity
import chana.jpql.nodes.Statement
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import scala.concurrent.duration._
import scala.concurrent.forkjoin.ThreadLocalRandom

object JPQLReporting {
  final case class ReportingTick(key: String)

  private def reportingDelay(interval: Duration) = ThreadLocalRandom.current.nextLong(100, interval.toMillis).millis
}

trait JPQLReporting extends Entity {
  import JPQLReporting._

  private var scheduledJpqls = List[String]()

  import context.dispatcher
  // TODO: 
  // 1. report once when new-created/new-jpql 
  // 2. report once when deleted  // in case of deleted, should guarantee via ACK etc?
  // 3. report only on updated 
  context.system.scheduler.schedule(1.seconds, 1.seconds, self, ReportingTick(""))

  def JPQLReportingBehavior: Receive = {
    case ReportingTick("") => scheduleJpqls()
    case ReportingTick(jpqlKey) =>
      DistributedJPQLBoard.keyToStatement.get(jpqlKey) match {
        case null             =>
        case (jpql, interval) => report(jpqlKey, jpql, interval, record)
      }
  }

  def eval(stmt: Statement, record: Record) = {
    val e = new JPQLEvaluator(stmt, record)
    e.visit()
  }

  def scheduleJpqls() {
    var newScheduledJpqls = List[String]()
    val jpqls = DistributedJPQLBoard.keyToStatement.entrySet.iterator
    while (jpqls.hasNext) {
      val entry = jpqls.next
      val key = entry.getKey
      if (!scheduledJpqls.contains(key)) {
        val value = entry.getValue
        report(key, value._1, value._2, record) // report at once
      }
      newScheduledJpqls ::= key
    }
    scheduledJpqls = newScheduledJpqls
  }

  def report(jpqlKey: String, jpql: Statement, interval: FiniteDuration, record: Record) {
    try {
      val res = if (isDeleted) null else eval(jpql, record)
      JPQLAggregator.aggregatorProxy(context.system, jpqlKey) ! JPQLAggregator.SelectToAggregator(id, res)
      context.system.scheduler.scheduleOnce(interval, self, ReportingTick(jpqlKey))
    } catch {
      case ex: Throwable => log.error(ex, ex.getMessage)
    }
  }

  // TODO
  def onUpdated_(fieldsBefore: Array[(Schema.Field, Any)], recordAfter: Record) {
    val jpqls = DistributedJPQLBoard.keyToStatement.entrySet.iterator
    while (jpqls.hasNext) {
      val entry = jpqls.next
      val jpqlId = entry.getKey
      val jpql = entry.getValue._1
    }
  }

  override def onDeleted() {
    super.onDeleted()
  }
}
