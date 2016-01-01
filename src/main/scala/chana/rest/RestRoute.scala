package chana.rest

import akka.actor.ActorSystem
import akka.contrib.pattern.ClusterSharding
import akka.contrib.pattern.DistributedPubSubExtension
import akka.contrib.pattern.DistributedPubSubMediator.Publish
import akka.pattern.ask
import akka.util.Timeout
import chana.PutJPQL
import chana.jpql
import chana.jpql.DistributedJPQLBoard
import chana.schema.DistributedSchemaBoard
import chana.script.DistributedScriptBoard
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.util.{ Failure, Success, Try }
import spray.http.{ StatusCode, StatusCodes }
import spray.routing.Directives

trait RestRoute extends Directives {
  val system: ActorSystem
  import system.dispatcher

  protected def mediator = DistributedPubSubExtension(system).mediator

  def readTimeout: Timeout
  def writeTimeout: Timeout

  def schemaBoard = DistributedSchemaBoard(system).board
  def scriptBoard = DistributedScriptBoard(system).board
  def jpqlBoard = DistributedJPQLBoard(system).board

  final def resolver(entityName: String) = ClusterSharding(system).shardRegion(entityName)

  final def restApi = schemaApi ~ jpqlApi ~ accessApi

  final def ping = path("ping") {
    complete("pong")
  }

  private def nextRandomId(min: Int, max: Int) = ThreadLocalRandom.current.nextInt(max - min + 1) + min

  final def schemaApi = {
    path("schema" / "put" / Segment ~ Slash.?) { entityName =>
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
    } ~ path("schema" / "del" / Segment ~ Slash.?) { entityName =>
      get {
        complete {
          withStatusCode {
            schemaBoard.ask(chana.RemoveSchema(entityName))(writeTimeout)
          }
        }
      }
    }
  }

  final def jpqlApi = {
    path("jpql") { // update/insert/delete 
      post {
        entity(as[String]) { jpqlQuery =>
          complete {
            withStatusCode {
              processJPQL("", jpqlQuery)
            }
          }
        }
      }
    } ~ path("jpql" / "put" / Segment ~ Slash.?) { key => // put select
      post {
        parameters('interval.as[Long].?) { intervalOpt =>
          entity(as[String]) { jpqlQuery =>
            complete {
              withStatusCode {
                val interval = intervalOpt.fold(10.second)(_.milliseconds)
                mediator ! Publish(jpql.JPQLBehavior.JPQLTopic, PutJPQL(key, jpqlQuery, interval))
                jpqlBoard.ask(chana.PutJPQL(key, jpqlQuery, interval))(writeTimeout)
              }
            }
          }
        }
      }
    } ~ path("jpql" / "del" / Segment ~ Slash.?) { key => // del select
      get {
        complete {
          withStatusCode {
            jpqlBoard.ask(chana.RemoveJPQL(key))(writeTimeout)
          }
        }
      }
    } ~ path("jpql" / "ask" / Segment ~ Slash.?) { key => // ask select
      get {
        complete {
          jpqlBoard.ask(chana.AskJPQL(key))(writeTimeout).collect {
            case Success(x) => x.toString
            case Failure(x) => x.toString
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
              case List(xpathExpr, _*) =>
                complete {
                  resolver(entityName).ask(chana.SelectJson(id, xpathExpr))(readTimeout).collect {
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
              case List(xpathExpr, valueJson) =>
                complete {
                  withStatusCode {
                    resolver(entityName).ask(chana.UpdateJson(id, xpathExpr, valueJson))(writeTimeout)
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
              case List(xpathExpr, json) =>
                complete {
                  withStatusCode {
                    resolver(entityName).ask(chana.InsertJson(id, xpathExpr, json))(writeTimeout)
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
              case List(xpathExpr, json) =>
                complete {
                  withStatusCode {
                    resolver(entityName).ask(chana.InsertAllJson(id, xpathExpr, json))(writeTimeout)
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
              case List(xpathExpr, _*) =>
                complete {
                  withStatusCode {
                    resolver(entityName).ask(chana.Delete(id, xpathExpr))(writeTimeout)
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
              case List(xpathExpr, _*) =>
                complete {
                  withStatusCode {
                    resolver(entityName).ask(chana.Clear(id, xpathExpr))(writeTimeout)
                  }
                }
              case _ =>
                complete(StatusCodes.BadRequest)
            }
          }
        }
      } ~ path("script" / "put" / Segment / Segment ~ Slash.?) { (field, scriptId) =>
        post {
          entity(as[String]) { script =>
            complete {
              withStatusCode {
                scriptBoard.ask(chana.PutScript(entityName, field, scriptId, script))(writeTimeout)
              }
            }
          }
        }
      } ~ path("script" / "del" / Segment / Segment ~ Slash.?) { (field, scriptId) =>
        get {
          complete {
            withStatusCode {
              scriptBoard.ask(chana.RemoveScript(entityName, field, scriptId))(writeTimeout)
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
        val xpathExpr = body.substring(0, i)
        val valueJson = body.substring(i + 2, len)
        List(xpathExpr, valueJson)
      } else {
        val xpathExpr = body.substring(0, i)
        val valueJson = body.substring(i + 1, len)
        List(xpathExpr, valueJson)
      }
    } else {
      i = body.indexOf('\n')
      if (i > 0) {
        val xpathExpr = body.substring(0, i)
        val valueJson = body.substring(i + 1, len)
        List(xpathExpr, valueJson)
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

  /**
   * We have to publish plain jpql query string instead of parsed AST, since:
   *   1. It's not efficient to serialize the AST
   *   2. When JsonNode is in AST, some of them is not be serializable
   */
  private def processJPQL(key: String, jpqlQuery: String): Future[Try[_]] = {
    jpql.parseJPQL(key, jpqlQuery) match {
      case Success(meta: jpql.JPQLSelect) =>
        // TODO will it happen? for select, we'll send to jpql board directly
        Future { Success(key) }

      case Success(meta: jpql.JPQLUpdate) =>
        // check if whereClause has id
        mediator ! Publish(jpql.JPQLBehavior.JPQLTopic, PutJPQL(key, jpqlQuery, Duration.Zero))
        Future { Success(key) }

      case Success(meta: jpql.JPQLInsert) =>
        // check id and sent to entity with id only
        mediator ! Publish(jpql.JPQLBehavior.JPQLTopic, PutJPQL(key, jpqlQuery, Duration.Zero))
        Future { Success(key) }

      case Success(meta: jpql.JPQLDelete) =>
        // check if whereClause has id
        mediator ! Publish(jpql.JPQLBehavior.JPQLTopic, PutJPQL(key, jpqlQuery, Duration.Zero))
        Future { Success(key) }

      case failure @ Failure(ex) =>
        //log.error(ex, ex.getMessage)
        Future { failure }
    }
  }

}
