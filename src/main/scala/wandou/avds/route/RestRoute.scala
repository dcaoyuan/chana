package wandou.avds.route

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import spray.http.StatusCodes
import spray.routing.Directives
import wandou.avds.script.DelScript
import wandou.avds.script.PutScript
import wandou.avds.script.ScriptBoard
import wandou.avpath
import wandou.avpath.Evaluator.Ctx
import wandou.avro.ToJson

trait RestRoute extends Directives {
  def system: ActorSystem
  def resolver: ActorRef
  def readTimeout: Timeout
  def writeTimeout: Timeout

  // use the enclosing ActorContext's or ActorSystem's dispatcher for our Futures and Scheduler
  implicit def executionContext: ExecutionContext = system.dispatcher

  lazy val scriptBoard = ScriptBoard(system).scriptBoard

  def restApi = {
    path("select" / Rest) { uid =>
      entity(as[String]) { body =>
        splitPathAndValue(body) match {
          case List(pathExp, _*) =>
            complete {
              resolver.ask(avpath.Select(uid, pathExp))(readTimeout).collect {
                case xs: List[_] =>
                  xs.map {
                    case x: Ctx => ToJson.toJsonString(x.value, x.schema)
                    case x      => x.toString
                  }.mkString("\n")
              }
            }
          case _ =>
            complete(StatusCodes.BadRequest)
        }
      }
    } ~
      path("update" / Rest) { uid =>
        post {
          entity(as[String]) { body =>
            splitPathAndValue(body) match {
              case List(pathExp, valueJson) =>
                complete {
                  resolver.ask(avpath.UpdateJson(uid, pathExp, valueJson))(writeTimeout).collect {
                    case Success(_)  => StatusCodes.OK
                    case Failure(ex) => StatusCodes.InternalServerError
                  }
                }
              case _ =>
                complete(StatusCodes.BadRequest)
            }
          }
        }
      } ~
      path("insert" / Rest) { uid =>
        post {
          entity(as[String]) { body =>
            splitPathAndValue(body) match {
              case List(pathExp, valueJson) =>
                complete {
                  resolver.ask(avpath.InsertJson(uid, pathExp, valueJson))(writeTimeout).collect {
                    case Success(_)  => StatusCodes.OK
                    case Failure(ex) => StatusCodes.InternalServerError
                  }
                }
              case _ =>
                complete(StatusCodes.BadRequest)
            }
          }
        }
      } ~
      path("insertall" / Rest) { uid =>
        post {
          entity(as[String]) { body =>
            splitPathAndValue(body) match {
              case List(pathExp, valueJson) =>
                complete {
                  resolver.ask(avpath.InsertAllJson(uid, pathExp, valueJson))(writeTimeout).collect {
                    case Success(_)  => StatusCodes.OK
                    case Failure(ex) => StatusCodes.InternalServerError
                  }
                }
              case _ =>
                complete(StatusCodes.BadRequest)
            }
          }
        }
      } ~
      path("delete" / Rest) { uid =>
        entity(as[String]) { body =>
          splitPathAndValue(body) match {
            case List(pathExp, _*) =>
              complete {
                resolver.ask(avpath.Delete(uid, pathExp))(writeTimeout).collect {
                  case Success(_)  => StatusCodes.OK
                  case Failure(ex) => StatusCodes.InternalServerError
                }
              }
            case _ =>
              complete(StatusCodes.BadRequest)
          }
        }
      } ~
      path("clear" / Rest) { uid =>
        entity(as[String]) { body =>
          splitPathAndValue(body) match {
            case List(pathExp, _*) =>
              complete {
                resolver.ask(avpath.Clear(uid, pathExp))(writeTimeout).collect {
                  case Success(_)  => StatusCodes.OK
                  case Failure(ex) => StatusCodes.InternalServerError
                }
              }
            case _ =>
              complete(StatusCodes.BadRequest)
          }
        }
      } ~
      path("putscript" / Rest) { scriptId =>
        entity(as[String]) { script =>
          complete {
            scriptBoard.ask(PutScript("Account", scriptId, script))(writeTimeout).collect {
              case Success(_)  => StatusCodes.OK
              case Failure(ex) => StatusCodes.InternalServerError
            }
          }
        }
      } ~
      path("delscript" / Rest) { scriptId =>
        entity(as[String]) { script =>
          complete {
            scriptBoard.ask(DelScript("Account", scriptId))(writeTimeout).collect {
              case Success(_)  => StatusCodes.OK
              case Failure(ex) => StatusCodes.InternalServerError
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
        val pathExp = body.substring(0, i)
        val valueJson = body.substring(i + 2, len)
        List(pathExp, valueJson)
      } else {
        val pathExp = body.substring(0, i)
        val valueJson = body.substring(i + 1, len)
        List(pathExp, valueJson)
      }
    } else {
      i = body.indexOf('\n')
      if (i > 0) {
        val pathExp = body.substring(0, i)
        val valueJson = body.substring(i + 1, len)
        List(pathExp, valueJson)
      } else {
        List(body)
      }
    }
  }

}
