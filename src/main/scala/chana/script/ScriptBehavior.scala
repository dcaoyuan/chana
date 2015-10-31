package chana.script

import akka.io.IO
import akka.pattern.ask
import chana.Entity
import javax.script.SimpleBindings
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import scala.concurrent.duration._
import spray.can.Http
import spray.httpx.RequestBuilding._

trait ScriptBehavior extends Entity {

  import context.dispatcher

  override def onUpdated(fieldsBefore: Array[(Schema.Field, Any)], recordAfter: Record) {
    super.onUpdated(fieldsBefore, recordAfter)

    val scripts = for {
      (field, _) <- fieldsBefore
      (_, script) <- DistributedScriptBoard.scriptsOf(entityName, field.name)
    } yield script

    val n = scripts.length
    if (n > 0) {
      var i = 0
      while (i < n) {
        try {
          log.debug("Current thread id [{}]", Thread.currentThread.getId)
          scripts(i).eval(prepareBindings(id, fieldsBefore, recordAfter))
        } catch {
          case ex: Throwable => log.error(ex, ex.getMessage)
        }
        i += 1
      }
    }
  }

  /**
   * NOTE: jdk.nashorn.internal.runtime.ConsString is CharSequence instead of String
   */
  val http_get = {
    (url: CharSequence) =>
      IO(Http)(context.system).ask(Get(url.toString))(5.seconds)
  }

  val http_post = {
    (url: CharSequence, body: CharSequence) =>
      IO(Http)(context.system).ask(Post(url.toString, body.toString))(5.seconds)
  }

  def prepareBindings(id: String, fieldsBefore: Array[(Schema.Field, Any)], recordAfter: Record) = {
    val bindings = new SimpleBindings()
    bindings.put("http_get", http_get)
    bindings.put("http_post", http_post)
    bindings.put("id", id)
    bindings.put("record", recordAfter)
    bindings.put("fields", fieldsBefore)
    bindings
  }
}
