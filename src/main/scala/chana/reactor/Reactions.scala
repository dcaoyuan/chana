package chana.reactor

import akka.actor.Actor.Receive
import akka.event.LoggingAdapter
import scala.collection.mutable.Buffer
import scala.collection.mutable.ListBuffer

/**
 * Used by Reactor to let clients register custom event reactions.
 */
final class Reactions(log: () => LoggingAdapter) extends Receive {
  private val parts: Buffer[Receive] = new ListBuffer[Receive]

  def isDefinedAt(e: Any) = parts.exists(_ isDefinedAt e)

  def apply(e: Any) {
    for (p <- parts if p isDefinedAt e) {
      try {
        p(e)
      } catch {
        case ex: Throwable =>
          log().error(ex, "Exception on message {}", e)
          throw ReactionsException(e, ex)
      }
    }
  }

  /**
   * Add a receive.
   */
  def +=(r: Receive): this.type = { parts += r; this }

  /**
   * Remove the given receive.
   */
  def -=(r: Receive): this.type = { parts -= r; this }

}

final case class ReactionsException(actorMessage: Any, originalException: Throwable) extends RuntimeException("Exception on message: " + actorMessage)
