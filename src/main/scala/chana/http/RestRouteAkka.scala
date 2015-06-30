package chana.http

import akka.actor.ActorSystem
import akka.contrib.pattern.ClusterSharding
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout
import chana.schema.DistributedSchemaBoard
import chana.script.DistributedScriptBoard
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Random
import scala.util.Success
import scala.util.Try

trait RestRouteAkka extends Directives {
  implicit val system: ActorSystem
  implicit val materializer = ActorMaterializer()
  import system.dispatcher

  def readTimeout: Timeout
  def writeTimeout: Timeout

  def schemaBoard = DistributedSchemaBoard(system).board
  def scriptBoard = DistributedScriptBoard(system).board

  final def resolver(entityName: String) = ClusterSharding(system).shardRegion(entityName)

  final def restApi = schemaApi ~ accessApi

  final def ping = path("ping") {
    complete("pong")
  }

  private val random = new Random()
  private def nextRandomId(min: Int, max: Int) = random.nextInt(max - min + 1) + min

  final def schemaApi = {
    pathPrefix("putschema") {
      path(Segment ~ Slash.?) { entityName =>
        post {
          parameters('fullname.as[String].?, 'timeout.as[Long].?) { (fullname, idleTimeout) =>
            entity(as[String]) { schemaStr =>
              complete {
                withStatusCode {
                  schemaBoard.ask(chana.PutSchema(entityName, schemaStr, fullname, idleTimeout.fold(Duration.Undefined: Duration)(_.milliseconds)))(writeTimeout)
                }
              }
            }
          }
        }
      }
    } ~ path("delschema" / Segment ~ Slash.?) { entityName =>
      get {
        complete {
          schemaBoard.ask(chana.RemoveSchema(entityName))(writeTimeout).collect {
            case Success(_)  => StatusCodes.OK
            case Failure(ex) => StatusCodes.InternalServerError
          }
        }
      }
    }
  }

  final def accessApi = {
    pathPrefix(Segment) { entityName =>
      pathPrefix("get") {
        path(Segment / Segment ~ Slash.?) { (id, fieldName) =>
          get {
            complete {
              withJson {
                resolver(entityName).ask(chana.GetFieldJson(id, fieldName))(readTimeout)
              }
            }
          }
        } ~ path(Segment ~ Slash.?) { id =>
          get {
            parameters('benchmark_only.as[Int].?) {
              case Some(benchmark_num) =>
                // Only for benchmark test purpose
                val shiftedId = nextRandomId(1, benchmark_num).toString
                complete {
                  withJson {
                    resolver(entityName).ask(chana.GetRecordJson(shiftedId))(readTimeout)
                  }
                }
              case _ =>
                complete {
                  withJson {
                    resolver(entityName).ask(chana.GetRecordJson(id))(readTimeout)
                  }
                }
            }
          }
        }
      } ~ pathPrefix("put") {
        path(Segment / Segment ~ Slash.?) { (id, fieldName) =>
          post {
            entity(as[String]) { json =>
              complete {
                withStatusCode {
                  resolver(entityName).ask(chana.PutFieldJson(id, fieldName, json))(writeTimeout)
                }
              }
            }
          }
        } ~ path(Segment ~ Slash.?) { id =>
          post {
            entity(as[String]) { json =>
              // Only for benchmark test purpose
              parameters('benchmark_only.as[Int].?) {
                case Some(benchmark_num) =>
                  val shiftedId = nextRandomId(1, benchmark_num).toString
                  complete {
                    withStatusCode {
                      resolver(entityName).ask(chana.PutRecordJson(shiftedId, json))(writeTimeout)
                    }
                  }
                case _ =>
                  complete {
                    withStatusCode {
                      resolver(entityName).ask(chana.PutRecordJson(id, json))(writeTimeout)
                    }
                  }
              }
            }
          }
        }
      } ~ path("select" / Segment ~ Slash.?) { id =>
        post {
          entity(as[String]) { body =>
            splitPathAndValue(body) match {
              case List(avpathExpr, _*) =>
                complete {
                  resolver(entityName).ask(chana.SelectJson(id, avpathExpr))(readTimeout).collect {
                    case Success(jsons: List[Array[Byte]] @unchecked) => jsons.map(new String(_)).mkString("[", ",", "]")
                    case Failure(ex)                                  => "[]"
                  }
                }
              case _ =>
                complete(StatusCodes.BadRequest)
            }
          }
        }
      } ~ path("update" / Segment ~ Slash.?) { id =>
        post {
          entity(as[String]) { body =>
            splitPathAndValue(body) match {
              case List(avpathExpr, valueJson) =>
                complete {
                  resolver(entityName).ask(chana.UpdateJson(id, avpathExpr, valueJson))(writeTimeout).collect {
                    case Success(_)  => StatusCodes.OK
                    case Failure(ex) => StatusCodes.InternalServerError
                  }
                }
              case _ =>
                complete(StatusCodes.BadRequest)
            }
          }
        }
      } ~ path("insert" / Segment ~ Slash.?) { id =>
        post {
          entity(as[String]) { body =>
            splitPathAndValue(body) match {
              case List(avpathExpr, json) =>
                complete {
                  resolver(entityName).ask(chana.InsertJson(id, avpathExpr, json))(writeTimeout).collect {
                    case Success(_)  => StatusCodes.OK
                    case Failure(ex) => StatusCodes.InternalServerError
                  }
                }
              case _ =>
                complete(StatusCodes.BadRequest)
            }
          }
        }
      } ~ path("insertall" / Segment ~ Slash.?) { id =>
        post {
          entity(as[String]) { body =>
            splitPathAndValue(body) match {
              case List(avpathExpr, json) =>
                complete {
                  resolver(entityName).ask(chana.InsertAllJson(id, avpathExpr, json))(writeTimeout).collect {
                    case Success(_)  => StatusCodes.OK
                    case Failure(ex) => StatusCodes.InternalServerError
                  }
                }
              case _ =>
                complete(StatusCodes.BadRequest)
            }
          }
        }
      } ~ path("delete" / Segment ~ Slash.?) { id =>
        post {
          entity(as[String]) { body =>
            splitPathAndValue(body) match {
              case List(avpathExpr, _*) =>
                complete {
                  resolver(entityName).ask(chana.Delete(id, avpathExpr))(writeTimeout).collect {
                    case Success(_)  => StatusCodes.OK
                    case Failure(ex) => StatusCodes.InternalServerError
                  }
                }
              case _ =>
                complete(StatusCodes.BadRequest)
            }
          }
        }
      } ~ path("clear" / Segment ~ Slash.?) { id =>
        post {
          entity(as[String]) { body =>
            splitPathAndValue(body) match {
              case List(avpathExpr, _*) =>
                complete {
                  resolver(entityName).ask(chana.Clear(id, avpathExpr))(writeTimeout).collect {
                    case Success(_)  => StatusCodes.OK
                    case Failure(ex) => StatusCodes.InternalServerError
                  }
                }
              case _ =>
                complete(StatusCodes.BadRequest)
            }
          }
        }
      } ~ path("putscript" / Segment / Segment ~ Slash.?) { (field, scriptId) =>
        post {
          entity(as[String]) { script =>
            complete {
              scriptBoard.ask(chana.PutScript(entityName, field, scriptId, script))(writeTimeout).collect {
                case Success(_)  => StatusCodes.OK
                case Failure(ex) => StatusCodes.InternalServerError
              }
            }
          }
        }
      } ~ path("delscript" / Segment / Segment ~ Slash.?) { (field, scriptId) =>
        get {
          complete {
            scriptBoard.ask(chana.RemoveScript(entityName, field, scriptId))(writeTimeout).collect {
              case Success(_)  => StatusCodes.OK
              case Failure(ex) => StatusCodes.InternalServerError
            }
          }
        }
      }
    }
  }

  private def splitPathAndValue(body: String): List[String] = {
    val len = body.length
    var i = body.indexOf('\r')
    if (i > 0) {
      if (i + 1 < len && body.charAt(i + 1) == '\n') {
        val avpathExpr = body.substring(0, i)
        val valueJson = body.substring(i + 2, len)
        List(avpathExpr, valueJson)
      } else {
        val avpathExpr = body.substring(0, i)
        val valueJson = body.substring(i + 1, len)
        List(avpathExpr, valueJson)
      }
    } else {
      i = body.indexOf('\n')
      if (i > 0) {
        val avpathExpr = body.substring(0, i)
        val valueJson = body.substring(i + 1, len)
        List(avpathExpr, valueJson)
      } else {
        List(body)
      }
    }
  }

  private def withStatusCode(f: => Future[Any]): Future[StatusCode] = f.mapTo[Try[String]].map {
    case Success(_) => StatusCodes.OK
    case Failure(_) => StatusCodes.InternalServerError
  }

  private def withJson(f: => Future[Any]): Future[String] = f.mapTo[Try[Array[Byte]]].map {
    case Success(json: Array[Byte]) => new String(json)
    case Failure(_)                 => ""
  }
}
