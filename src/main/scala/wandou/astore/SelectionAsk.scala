package wandou.astore

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorRefFactory
import akka.actor.Props
import akka.actor.ReceiveTimeout
import scala.concurrent.duration.Deadline
import scala.concurrent.duration.FiniteDuration

/**
 * Message to send to the temp actor that handles request/response to the selection
 */
case class AskSelection(path: String, msg: Any, askTimeout: FiniteDuration)

/**
 * Factory to create the selection asker
 */
object SelectionAsker {
  def apply(fact: ActorRefFactory) = fact.actorOf(Props[SelectionAsker])
}

/**
 * Actor that handles the request to aggregate responses from a selection
 */
class SelectionAsker extends Actor {
  import context._
  var responses: List[Any] = List.empty

  def receive = waitingForRequest

  def waitingForRequest: Receive = {
    case request @ AskSelection(path, msg, askTO) =>
      system.actorSelection(path) ! msg
      setReceiveTimeout(askTO)
      become(waitingForResponses(sender(), askTO.fromNow))
  }

  def waitingForResponses(originator: ActorRef, deadline: Deadline): Receive = {
    case ReceiveTimeout =>
      originator ! responses
      context.stop(self)
    case any =>
      responses = any :: responses
      setReceiveTimeout(deadline.timeLeft)
  }
}
