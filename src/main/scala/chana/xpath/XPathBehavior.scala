package chana.avpath

import chana.Clear
import chana.Delete
import chana.Entity
import chana.Insert
import chana.InsertAll
import chana.InsertAllJson
import chana.InsertJson
import chana.Select
import chana.SelectAvro
import chana.SelectJson
import chana.Update
import chana.UpdateJson
import chana.xpath
import chana.xpath.Ctx
import scala.util.Failure
import scala.util.Success
import scala.util.Try

trait XPathBehavior extends Entity {

  def avpathBehavior: Receive = {
    case Select(_, path) =>
      resetIdleTimeout()
      val commander = sender()
      xpath.select(record, path) match {
        case Success(ctxList: List[Ctx]) =>
          commander ! Success(ctxList)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case SelectAvro(_, path) =>
      resetIdleTimeout()
      val commander = sender()
      xpath.select(record, path) match {
        case x @ Success(ctxs) =>
          Try {
            ctxs.map { ctx => encoderDecoder.avroEncode(ctx.value, ctx.schema).get }
          } match {
            case xs: Success[_] =>
              commander ! xs // List[Array[Byte]] 
            case x @ Failure(ex) =>
              log.error(ex, ex.getMessage)
              commander ! x
          }
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case SelectJson(_, path) =>
      resetIdleTimeout()
      val commander = sender()
      xpath.select(record, path) match {
        case Success(ctxs) =>
          Try {
            ctxs.map { ctx => encoderDecoder.jsonEncode(ctx.value, ctx.schema).get }
          } match {
            case xs: Success[_] =>
              commander ! xs // List[Array[Byte]]
            case x @ Failure(ex) =>
              log.error(ex, ex.getMessage)
              commander ! x
          }
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case Update(_, path, value) =>
      resetIdleTimeout()
      val commander = sender()
      xpath.update(record, path, value) match {
        case Success(actions) =>
          commit(id, actions, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case UpdateJson(_, path, value) =>
      resetIdleTimeout()
      val commander = sender()
      xpath.updateJson(record, path, value) match {
        case Success(actions) =>
          commit(id, actions, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case Insert(_, path, value) =>
      resetIdleTimeout()
      val commander = sender()
      xpath.insert(record, path, value) match {
        case Success(actions) =>
          commit(id, actions, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case InsertJson(_, path, value) =>
      resetIdleTimeout()
      val commander = sender()
      xpath.insertJson(record, path, value) match {
        case Success(actions) =>
          commit(id, actions, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case InsertAll(_, path, values) =>
      resetIdleTimeout()
      val commander = sender()
      xpath.insertAll(record, path, values) match {
        case Success(actions) =>
          commit(id, actions, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case InsertAllJson(_, path, values) =>
      resetIdleTimeout()
      val commander = sender()
      xpath.insertAllJson(record, path, values) match {
        case Success(actions) =>
          commit(id, actions, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case Delete(_, path) =>
      resetIdleTimeout()
      val commander = sender()
      xpath.delete(record, path) match {
        case Success(actions) =>
          commit(id, actions, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case Clear(_, path) =>
      resetIdleTimeout()
      val commander = sender()
      xpath.clear(record, path) match {
        case Success(actions) =>
          commit(id, actions, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

  }
}
