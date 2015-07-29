package chana.jpql

import akka.actor.Actor
import akka.event.LoggingAdapter
import chana.jpql.nodes.Statement
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import scala.concurrent.duration._

object JPQLReporting {
  case object Reporting
}

trait JPQLReporting extends Actor {
  def log: LoggingAdapter
  def entityName: String

  import context.dispatcher
  // TODO change to onUpated 
  val reportingTask = Some(context.system.scheduler.schedule(0.seconds, 1.seconds, self, JPQLReporting.Reporting))

  def eval(stmt: Statement, record: Record) = {
    val e = new JPQLEvaluator(stmt, record)
    e.visit()
  }

  def onQuery(id: String, record: Record) {
    val jpqls = DistributedJPQLBoard.keyToStatement.entrySet.iterator
    while (jpqls.hasNext) {
      val entry = jpqls.next
      val key = entry.getKey
      val jpql = entry.getValue._1
      val res = eval(jpql, record)
      JPQLAggregator.aggregatorProxy(context.system, key) ! JPQLAggregator.SelectToAggregator(id, res)
    }
  }

  // TODO
  def onUpdated_(id: String, fieldsBefore: Array[(Schema.Field, Any)], recordAfter: Record) {
    val jpqls = DistributedJPQLBoard.keyToStatement.entrySet.iterator
    while (jpqls.hasNext) {
      val entry = jpqls.next
      val jpqlId = entry.getKey
      val jpql = entry.getValue._1
    }
  }
}
