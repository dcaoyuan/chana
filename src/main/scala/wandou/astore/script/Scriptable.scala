package wandou.astore.script

import akka.actor.Actor
import akka.actor.Stash
import akka.event.LoggingAdapter
import akka.io.IO
import akka.pattern.ask
import javax.script.SimpleBindings
import scala.concurrent.duration._
import spray.can.Http
import spray.httpx.RequestBuilding._
import wandou.astore.Entity

object Scriptable {
  private case object ScriptFinished
  private case object ScriptTimeout
}
trait Scriptable extends Actor with Stash {

  def log: LoggingAdapter
  def entityName: String

  import context.dispatcher

  def scriptableBehavior: Receive = {
    case x @ Entity.OnUpdated(id, fieldsBefore, recordAfter) =>
      for {
        (field, _) <- fieldsBefore
        (_, script) <- DistributedScriptBoard.scriptsOf(entityName, field.name)
      } {
        // going to wait for scripting finished
        context.become {
          case Scriptable.ScriptFinished | Scriptable.ScriptTimeout =>
            unstashAll()
            context.unbecome()
          case _ =>
            stash()
        }
        context.system.scheduler.scheduleOnce(5.seconds, self, Scriptable.ScriptTimeout)

        try {
          script.eval(prepareBindings(x))
        } catch {
          case ex: Throwable =>
            self ! Scriptable.ScriptFinished
            log.error(ex, ex.getMessage)
        }
      }
  }

  /**
   * Note: jdk.nashorn.internal.runtime.ConsString is CharSequence instead of String
   */
  val http_get = {
    (url: CharSequence) =>
      IO(Http)(context.system).ask(Get(url.toString))(5.seconds)
  }

  val http_post = {
    (url: CharSequence, body: CharSequence) =>
      IO(Http)(context.system).ask(Post(url.toString, body.toString))(5.seconds)
  }

  val notify_finished = {
    () =>
      self ! Scriptable.ScriptFinished
  }

  def prepareBindings(onUpdated: Entity.OnUpdated) = {
    val bindings = new SimpleBindings()
    bindings.put("notify_finished", notify_finished)
    bindings.put("http_get", http_get)
    bindings.put("http_post", http_post)
    bindings.put("id", onUpdated.id)
    bindings.put("record", onUpdated.recordAfter)
    bindings.put("fields", onUpdated.fieldsBefore)
    bindings
  }
}
