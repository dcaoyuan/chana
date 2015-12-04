package chana.script

import akka.io.IO
import akka.pattern.ask
import chana.Entity
import chana.avro.UpdateEvent
import chana.avro.Binlog
import javax.script.SimpleBindings
import scala.concurrent.duration._
import spray.can.Http
import spray.httpx.RequestBuilding._

trait ScriptBehavior extends Entity {

  import context.dispatcher

  override def onUpdated(event: UpdateEvent) {
    super.onUpdated(event)

    val scripts = if (event.binlogs.map(_.xpath).contains("/")) { // eval all scripts?
      val xs = for {
        (_, script) <- DistributedScriptBoard.scriptsOf(entityName)
      } yield script
      xs.toArray
    } else {
      val xs = for {
        binlog @ Binlog(tpe, xpath, value) <- event.binlogs
        (_, script) <- DistributedScriptBoard.scriptsOf(entityName, xpath)
      } yield script
      xs
    }

    val n = scripts.length
    var i = 0
    while (i < n) {
      try {
        log.debug("Current thread id [{}]", Thread.currentThread.getId)
        scripts(i).eval(prepareBindings(id, event.binlogs))
      } catch {
        case ex: Throwable => log.error(ex, ex.getMessage)
      }
      i += 1
    }
  }

  /**
   * NOTE: jdk.nashorn.internal.runtime.ConsString is CharSequence instead of String
   */
  val http_get = {
    (url: CharSequence, timeout: Int) =>
      IO(Http)(context.system).ask(Get(url.toString))(timeout.seconds)
  }

  val http_post = {
    (url: CharSequence, body: CharSequence, timeout: Int) =>
      IO(Http)(context.system).ask(Post(url.toString, body.toString))(timeout.seconds)
  }

  def prepareBindings(id: String, binlogs: Array[Binlog]) = {
    val bindings = new SimpleBindings()
    bindings.put("http_get", http_get)
    bindings.put("http_post", http_post)
    bindings.put("id", id)
    bindings.put("record", record)
    bindings.put("binlogs", binlogs)
    bindings
  }
}
