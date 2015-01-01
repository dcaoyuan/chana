package wandou.astore

import akka.actor.{ Props, ActorLogging }
import spray.routing.{ HttpServiceActor, Route }

object HttpServer {
  def props(route: Route): Props = Props(classOf[HttpServer], route)
}

class HttpServer(route: Route) extends HttpServiceActor with ActorLogging {
  override def receive: Receive = runRoute(sealRoute(route))
}
