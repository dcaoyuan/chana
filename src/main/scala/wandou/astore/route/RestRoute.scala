package wandou.astore.route

import akka.actor.ActorSystem
import akka.contrib.pattern.ClusterSharding
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Random
import scala.util.Success
import spray.http.StatusCodes
import spray.routing.Directives
import wandou.astore
import wandou.astore.schema.DistributedSchemaBoard
import wandou.astore.script.DistributedScriptBoard

trait RestRoute { _: spray.routing.Directives =>
  val system: ActorSystem
  def readTimeout: Timeout
  def writeTimeout: Timeout

  def schemaBoard = DistributedSchemaBoard(system).board
  def scriptBoard = DistributedScriptBoard(system).board

  import system.dispatcher

  final def resolver(entityName: String) = ClusterSharding(system).shardRegion(entityName)

  final def restApi = schemaApi ~ accessApi

  final def ping = path("ping") {
    println("PONG")
    complete("pong")
  }

  private val random = new Random()
  private def nextRandomId(min: Int, max: Int) = random.nextInt(max - min + 1) + min

  final def schemaApi = {
    path("putschema" / Segment / Segment ~ Slash.?) { (entityName, fname) =>
      post {
        entity(as[String]) { schemaStr =>
          complete {
            schemaBoard.ask(astore.PutSchema(entityName, schemaStr, Some(fname)))(writeTimeout).collect {
              case Success(_)  => StatusCodes.OK
              case Failure(ex) => StatusCodes.InternalServerError
            }
          }
        }
      }
    } ~ path("putschema" / Segment ~ Slash.?) { entityName =>
      post {
        entity(as[String]) { schemaStr =>
          complete {
            schemaBoard.ask(astore.PutSchema(entityName, schemaStr, None))(writeTimeout).collect {
              case Success(_)  => StatusCodes.OK
              case Failure(ex) => StatusCodes.InternalServerError
            }
          }
        }
      }
    } ~ path("delschema" / Segment ~ Slash.?) { entityName =>
      get {
        complete {
          schemaBoard.ask(astore.RemoveSchema(entityName))(writeTimeout).collect {
            case Success(_)  => StatusCodes.OK
            case Failure(ex) => StatusCodes.InternalServerError
          }
        }
      }
    }
  }

  final def accessApi = {
    pathPrefix(Segment) { entityName =>
      path("get" / Segment / Segment ~ Slash.?) { (id, fieldName) =>
        get {
          complete {
            resolver(entityName).ask(astore.GetFieldJson(id, fieldName))(readTimeout).collect {
              case Success(json: String) => json
              case Failure(ex)           => ""
            }
          }
        }
      } ~ path("get" / Segment ~ Slash.?) { id =>
        get {
          parameters('benchmark_only.as[String]) { benchmark =>
            val shiftedId = nextRandomId(1, 1024).toString
            complete {
              resolver(entityName).ask(astore.GetRecordJson(shiftedId))(readTimeout).collect {
                case Success(json: String) => json
                case Failure(ex)           => ""
              }
            }
          } ~
            complete {
              resolver(entityName).ask(astore.GetRecordJson(id))(readTimeout).collect {
                case Success(json: String) => json
                case Failure(ex)           => ""
              }
            }
        }
      } ~ path("put" / Segment / Segment ~ Slash.?) { (id, fieldName) =>
        post {
          entity(as[String]) { json =>
            complete {
              resolver(entityName).ask(astore.PutFieldJson(id, fieldName, json))(writeTimeout).collect {
                case Success(_)  => StatusCodes.OK
                case Failure(ex) => StatusCodes.InternalServerError
              }
            }
          }
        }
      } ~ path("put" / Segment ~ Slash.?) { id =>
        post {
          entity(as[String]) { json =>
            complete {
              resolver(entityName).ask(astore.PutRecordJson(id, json))(writeTimeout).collect {
                case Success(_)  => StatusCodes.OK
                case Failure(ex) => StatusCodes.InternalServerError
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
                  resolver(entityName).ask(astore.SelectJson(id, avpathExpr))(readTimeout).collect {
                    case Success(json: String) => json
                    case Failure(ex)           => "[]"
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
                  resolver(entityName).ask(astore.UpdateJson(id, avpathExpr, valueJson))(writeTimeout).collect {
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
                  resolver(entityName).ask(astore.InsertJson(id, avpathExpr, json))(writeTimeout).collect {
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
                  resolver(entityName).ask(astore.InsertAllJson(id, avpathExpr, json))(writeTimeout).collect {
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
                  resolver(entityName).ask(astore.Delete(id, avpathExpr))(writeTimeout).collect {
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
                  resolver(entityName).ask(astore.Clear(id, avpathExpr))(writeTimeout).collect {
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
              scriptBoard.ask(astore.PutScript(entityName, field, scriptId, script))(writeTimeout).collect {
                case Success(_)  => StatusCodes.OK
                case Failure(ex) => StatusCodes.InternalServerError
              }
            }
          }
        }
      } ~ path("delscript" / Segment / Segment ~ Slash.?) { (field, scriptId) =>
        get {
          complete {
            scriptBoard.ask(astore.RemoveScript(entityName, field, scriptId))(writeTimeout).collect {
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

}
