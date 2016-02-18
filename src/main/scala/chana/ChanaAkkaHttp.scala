package chana

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Directives
import akka.stream.ActorMaterializer
import akka.util.Timeout
import chana.rest.RestRouteAkka
import scala.concurrent.duration._

/**
 * Chana REST service
 */
object ChanaAkkaHttp extends scala.App {
  implicit val system = ActorSystem("ChanaSystem")
  implicit val materializer = ActorMaterializer()
  implicit val dispatcher = system.dispatcher

  val route = Directives.respondWithHeader(RawHeader("Access-Control-Allow-Origin", "*")) {
    new ChanaRouteAkka(system).route
  }

  val webConfig = system.settings.config.getConfig("chana.web")
  val source = Http().bind(webConfig.getString("interface"), webConfig.getInt("port"))
  source.runForeach { conn =>
    conn.handleWith(route)
  }
}

class ChanaRouteAkka(val system: ActorSystem) extends RestRouteAkka with Directives {
  val readTimeout: Timeout = 5.seconds
  val writeTimeout: Timeout = 5.seconds

  val route = ping ~ restApi
}

