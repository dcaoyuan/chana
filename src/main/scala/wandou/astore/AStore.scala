package wandou.astore

import akka.actor.ActorSystem
import akka.io.IO
import akka.util.Timeout
import scala.concurrent.duration._
import spray.can.Http
import spray.http.HttpHeaders.RawHeader
import spray.routing.Directives
import wandou.astore.route.RestRoute
import wandou.astore.schema.ClusterSchemaBoard
import wandou.astore.schema.NodeSchemaBoard
import wandou.astore.script.ClusterScriptBoard
import wandou.astore.script.NodeScriptBoard

/**
 * Start REST astore service
 */
object AStore extends scala.App {
  implicit val system = ActorSystem("AStoreSystem")

  ClusterSchemaBoard.startClusterSchemaBoard(system, None)
  ClusterSchemaBoard.startClusterSchemaBoardProxy(system, None)
  NodeSchemaBoard(system).schemaBoard // start NodeSchemaBoard's local schemaBoard 

  ClusterScriptBoard.startClusterScriptBoard(system, None)
  ClusterScriptBoard.startClusterScriptBoardProxy(system, None)
  NodeScriptBoard(system).scriptBoard // start NodeScriptBoard's local schemaBoard 

  val routes = Directives.respondWithHeader(RawHeader("Access-Control-Allow-Origin", "*")) {
    new AStoreRoute(system).route
  }

  val server = system.actorOf(HttpServer.props(routes), "astore-web")
  val webConfig = system.settings.config.getConfig("web")
  IO(Http) ! Http.Bind(server, webConfig.getString("hostname"), port = webConfig.getInt("port"))

}

class AStoreRoute(val system: ActorSystem) extends RestRoute with Directives {
  def clusterSchemaBoardProxy = ClusterSchemaBoard.clusterSchemaBoardProxy(system)
  def clusterScriptBoardProxy = ClusterScriptBoard.clusterScriptBoardProxy(system)
  val readTimeout: Timeout = 20.seconds
  val writeTimeout: Timeout = 20.seconds

  val route = restApi
}
