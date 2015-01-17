package wandou.astore.script

import akka.actor.Actor
import akka.actor.Actor.Receive
import akka.event.LoggingAdapter
import akka.io.IO
import akka.pattern.ask
import javax.script.SimpleBindings
import scala.collection
import scala.concurrent.duration._
import spray.can.Http
import spray.httpx.RequestBuilding._
import wandou.astore.Entity.OnUpdated

trait Scriptable { me: Actor =>

  def log: LoggingAdapter
  def name: String

  import context.dispatcher

  def scriptableBehavior: Receive = {
    case x @ OnUpdated(id, fieldsBefore, recordAfter) =>
      for {
        (field, _) <- fieldsBefore
        (id, script) <- DistributedScriptBoard.scriptsOf(name, field.name)
      } {
        try {
          script.eval(prepareBindings(x))
        } catch {
          case ex: Throwable => log.error(ex, ex.getMessage)
        }
      }
  }

  val http_get = {
    (url: String) =>
      IO(Http)(context.system).ask(Get(url))(1.seconds).onComplete {
        case x => log.debug("Response of http_get: {}", x)
      }
  }

  val http_post = {
    (url: String, body: String) =>
      IO(Http)(context.system).ask(Post(url, body))(1.seconds).onComplete {
        case x => log.debug("Response of http_post: {}", x)
      }
  }

  def prepareBindings(onUpdated: OnUpdated) = {
    val bindings = new SimpleBindings
    bindings.put("http_get", http_get)
    bindings.put("http_post", http_post)
    bindings.put("id", onUpdated.id)
    bindings.put("record", onUpdated.recordAfter)
    bindings.put("fields", onUpdated.fieldsBefore)
    bindings
  }
}
