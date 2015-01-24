package wandou.astore

import akka.actor.ActorSystem
import akka.http.Http
import akka.http.model.headers.RawHeader
import akka.http.server.Directives
import akka.stream.FlowMaterializer
import akka.util.Timeout
import scala.concurrent.duration._
import wandou.astore.route.RestRouteAkka

/**
 * astore REST service
 */
object AStoreAkkaHttp extends scala.App {
  implicit val system = ActorSystem("AStoreSystem")
  implicit val materializer = FlowMaterializer()
  implicit val dispatcher = system.dispatcher

  val routes = Directives.respondWithHeader(RawHeader("Access-Control-Allow-Origin", "*")) {
    new AStoreRouteAkka(system).route
  }

  val webConfig = system.settings.config.getConfig("wandou.astore.web")
  val serverBinding = Http().bind(interface = webConfig.getString("hostname"), port = webConfig.getInt("port"))
  serverBinding.startHandlingWith(routes)
}

class AStoreRouteAkka(val system: ActorSystem) extends RestRouteAkka with Directives {
  val readTimeout: Timeout = 5.seconds
  val writeTimeout: Timeout = 5.seconds

  val route = ping ~ restApi
}

