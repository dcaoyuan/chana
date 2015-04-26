package wandou.astore

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Directives
import akka.stream.ActorFlowMaterializer
import akka.util.Timeout
import scala.concurrent.duration._
import wandou.astore.http.RestRouteAkka

/**
 * AStore REST service
 */
object AStoreAkkaHttp extends scala.App {
  implicit val system = ActorSystem("AStoreSystem")
  implicit val materializer = ActorFlowMaterializer()
  implicit val dispatcher = system.dispatcher

  val route = Directives.respondWithHeader(RawHeader("Access-Control-Allow-Origin", "*")) {
    new AStoreRouteAkka(system).route
  }

  val webConfig = system.settings.config.getConfig("wandou.astore.web")
  val source = Http().bind(interface = webConfig.getString("interface"), port = webConfig.getInt("port"))
  source.runForeach { conn =>
    conn.handleWith(route)
  }
}

class AStoreRouteAkka(val system: ActorSystem) extends RestRouteAkka with Directives {
  val readTimeout: Timeout = 5.seconds
  val writeTimeout: Timeout = 5.seconds

  val route = ping ~ restApi
}

