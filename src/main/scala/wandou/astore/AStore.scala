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

/**
 *
 * $ cd src/test/resources/avsc
 * $ curl --data @LongList.record 'http://localhost:8080/putschema/longlist'
 * $ curl 'http://localhost:8080/longlist/get/1'
 * $ curl --data-binary @update.value 'http://localhost:8080/longlist/update/1'
 * $ curl 'http://localhost:8080/longlist/get/1'
 * $ weighttp -c100 -n100000 -k 'http://localhost:8080/longlist/get/1'
 *
 */
object AStore extends scala.App {
  implicit val system = ActorSystem("AStoreSystem")

  ClusterSchemaBoard.startClusterSchemaBoard(system, None)
  ClusterSchemaBoard.startClusterSchemaBoardProxy(system, None)
  NodeSchemaBoard(system).schemaBoard // start NodeSchemaBoard's local schemaBoard 

  val routes = Directives.respondWithHeader(RawHeader("Access-Control-Allow-Origin", "*")) {
    new AStoreRoute(system).route
  }

  val server = system.actorOf(HttpServer.props(routes), "astore-web")
  val webConfig = system.settings.config.getConfig("web")
  IO(Http) ! Http.Bind(server, webConfig.getString("hostname"), port = webConfig.getInt("port"))

}

class AStoreRoute(val system: ActorSystem) extends RestRoute with Directives {
  def clusterSchemaBoardProxy = ClusterSchemaBoard.clusterSchemaBoardProxy(system)
  val readTimeout: Timeout = 20.seconds
  val writeTimeout: Timeout = 20.seconds

  val route = restApi
}
