package wandou.astore

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.Props
import akka.io.IO
import akka.util.Timeout
import scala.concurrent.duration._
import spray.can.Http
import spray.http.HttpHeaders.RawHeader
import spray.routing.Directives
import spray.routing.HttpServiceActor
import spray.routing.Route
import wandou.astore.http.RestRoute

/**
 * Start REST astore service
 */
object AStore extends scala.App {
  implicit val system = ActorSystem("AStoreSystem")

  val route = Directives.respondWithHeader(RawHeader("Access-Control-Allow-Origin", "*")) {
    new AStoreRoute(system).route
  }

  val server = system.actorOf(RestServer.props(route), "astore-web")
  val webConfig = system.settings.config.getConfig("wandou.astore.web")
  IO(Http) ! Http.Bind(server, webConfig.getString("interface"), port = webConfig.getInt("port"))

}

class AStoreRoute(val system: ActorSystem) extends RestRoute with Directives {
  val readTimeout: Timeout = 5.seconds
  val writeTimeout: Timeout = 5.seconds

  val route = ping ~ restApi
}

object RestServer {
  def props(route: Route) = Props(classOf[RestServer], route)
}
class RestServer(route: Route) extends Actor with ActorLogging {
  def receive = {
    // when a new connection comes in we register a worker actor as the per connection handler
    case Http.Connected(remoteAddress, localAddress) =>
      val serverConnection = sender()
      val conn = context.actorOf(RestWorker.props(route))
      serverConnection ! Http.Register(conn)
  }
}

object RestWorker {
  def props(route: Route): Props = Props(classOf[RestWorker], route)
}
class RestWorker(route: Route) extends HttpServiceActor with ActorLogging {
  override def receive: Receive = runRoute(sealRoute(route))
}
