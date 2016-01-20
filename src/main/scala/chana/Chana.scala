package chana

import akka.actor.{ ActorLogging, Actor, Props, ActorSystem }
import akka.io.IO
import akka.persistence.Persistence
import akka.util.Timeout
import chana.rest.RestRoute
import spray.can.Http
import spray.http.HttpHeaders.RawHeader
import spray.routing.{ Directives, HttpServiceActor, Route }
import scala.concurrent.duration._

/**
 * Start REST chana service
 */
object Chana extends scala.App {
  implicit val system = ActorSystem("ChanaSystem")

  val route = Directives.respondWithHeader(RawHeader("Access-Control-Allow-Origin", "*")) {
    new ChanaRoute(system).route
  }

  val server = system.actorOf(RestServer.props(route), "chana-web")
  val webConfig = system.settings.config.getConfig("chana.web")
  Persistence(system)
  IO(Http) ! Http.Bind(server, webConfig.getString("interface"), webConfig.getInt("port"))
}

final class ChanaRoute(val system: ActorSystem) extends RestRoute with Directives {
  val readTimeout: Timeout = 5.seconds
  val writeTimeout: Timeout = 5.seconds

  val route = ping ~ restApi
}

object RestServer {
  def props(route: Route) = Props(classOf[RestServer], route)
}
final class RestServer(route: Route) extends Actor with ActorLogging {
  def receive = {
    // when a new connection comes in we register a worker actor as the per connection handler
    case Http.Connected(remoteAddress, localAddress) =>
      val serverConnection = sender()
      val worker = context.actorOf(RestWorker.props(route))
      serverConnection ! Http.Register(worker)
  }
}

object RestWorker {
  def props(route: Route): Props = Props(classOf[RestWorker], route)
}
final class RestWorker(route: Route) extends HttpServiceActor with ActorLogging {
  override def receive: Receive = runRoute(sealRoute(route))
}
