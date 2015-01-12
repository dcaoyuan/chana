package wandou.astore

import akka.actor.ActorSystem
import akka.io.IO
import akka.util.Timeout
import scala.concurrent.duration._
import spray.can.Http
import spray.http.HttpHeaders.RawHeader
import spray.routing.Directives
import wandou.astore.route.RestRoute

/**
 * Start REST astore service
 */
object AStore extends scala.App {
  implicit val system = ActorSystem("AStoreSystem")

  val routes = Directives.respondWithHeader(RawHeader("Access-Control-Allow-Origin", "*")) {
    new AStoreRoute(system).route
  }

  val server = system.actorOf(HttpServer.props(routes), "astore-web")
  val webConfig = system.settings.config.getConfig("wandou.astore.web")
  IO(Http) ! Http.Bind(server, webConfig.getString("hostname"), port = webConfig.getInt("port"))

}

class AStoreRoute(val system: ActorSystem) extends RestRoute with Directives {
  val readTimeout: Timeout = 5.seconds
  val writeTimeout: Timeout = 5.seconds

  val route = ping ~ restApi
}
