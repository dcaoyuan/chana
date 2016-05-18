package chana

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.server.RouteResult.Complete
import akka.http.scaladsl.server.directives.LogEntry
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

final class ChanaRouteAkka(val system: ActorSystem)(implicit val m: ActorMaterializer) extends RestRouteAkka with Directives {
  val readTimeout: Timeout = 5.seconds
  val writeTimeout: Timeout = 5.seconds
  import system.dispatcher

  def route = ping ~ restApi

  def routeWithLogging = logAccess(route)

  private def logAccess = logRequestResult(accessLogFunc)
  private def accessLogFunc: HttpRequest => (Any => Option[LogEntry]) = { req =>
    // we should remember request first, at here
    val requestTime = System.currentTimeMillis
    val func: Any => Option[LogEntry] = {
      case Complete(resp) =>
        Some(LogEntry(
          s"Access log: ${req.method} ${req.uri} ${resp.status} " +
            s"${System.currentTimeMillis - requestTime} ${resp.entity.contentLengthOption.getOrElse(-1)}", Logging.InfoLevel))
      case _ => None
    }
    func
  }
}

