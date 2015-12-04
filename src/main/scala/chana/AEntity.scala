package chana

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Props
import chana.avpath.AVPathBehavior
import chana.avro.DefaultRecordBuilder
import chana.avro.UpdateEvent
import chana.jpql.JPQLBehavior
import chana.script.ScriptBehavior
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

object AEntity {
  def props(entityName: String, schema: Schema, builder: DefaultRecordBuilder, idleTimeout: Duration) =
    Props(classOf[AEntity], entityName, schema, builder, idleTimeout)
}

/**
 * A typical entity class.
 * You can choose which deatures that you want by defining your custom Entity.
 */
class AEntity(val entityName: String, val schema: Schema, val builder: DefaultRecordBuilder, idleTimeout: Duration)
    extends Entity
    with AVPathBehavior
    with ScriptBehavior
    with JPQLBehavior
    with Actor
    with ActorLogging {

  idleTimeout match {
    case f: FiniteDuration =>
      setIdleTimeout(f)
      resetIdleTimeout()
    case _ =>
  }

  override def receiveCommand = accessBehavior orElse persistBehavior orElse avpathBehavior orElse jpqlBehavior

  override def onUpdated(event: UpdateEvent) {
    super[ScriptBehavior].onUpdated(event)
    super[JPQLBehavior].onUpdated(event)
  }
}
