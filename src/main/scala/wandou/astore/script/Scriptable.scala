package wandou.astore.script

import akka.actor.Actor
import akka.actor.Actor.Receive
import akka.event.LoggingAdapter
import javax.script.Bindings
import javax.script.CompiledScript
import scala.collection
import scala.concurrent.duration._

object Scriptable {
  object ScriptingTick
}

trait Scriptable { _: Actor =>
  lazy val scriptIdToScript = new collection.mutable.HashMap[String, CompiledScript]()

  def log: LoggingAdapter
  def entityName: String = this.getClass.getSimpleName
  def prepareBindings: Bindings

  import context.dispatcher
  val scriptingTask = Some(context.system.scheduler.schedule(5.seconds, 5.seconds, self, Scriptable.ScriptingTick))

  def scriptableBehavior: Receive = {
    case Scriptable.ScriptingTick =>
      ScriptBoard.scriptsOf(entityName) foreach {
        case (id, script) =>
          try {
            script.eval(prepareBindings)
          } catch {
            case ex: Throwable => log.error(ex, ex.getMessage)
          }
      }
  }
}