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
import chana.avpath
import chana.avpath.Evaluator.Ctx
import org.apache.avro.generic.GenericData
import scala.util.Failure
import scala.util.Success
import scala.util.Try

@deprecated("Use XPathBehavior", "0.2")
trait AVPathBehavior extends Entity {
  //protected def parser = new avpath.Parser()

  def avpathBehavior: Receive = {
    case Select(_, path) =>
      resetIdleTimeout()
      val commander = sender()
      avpath.select(record, path) match {
        case Success(ctxList: List[Ctx]) =>
          commander ! Success(ctxList.map { ctx =>
            Ctx(GenericData.get().deepCopy(ctx.schema, ctx.value), ctx.schema, ctx.topLevelField, ctx.target)
          })
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case SelectAvro(_, path) =>
      resetIdleTimeout()
      val commander = sender()
      avpath.select(record, path) match {
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
      avpath.select(record, path) match {
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
      avpath.update(record, path, value) match {
        case Success(actions) =>
          commit(id, actions, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case UpdateJson(_, path, value) =>
      resetIdleTimeout()
      val commander = sender()
      avpath.updateJson(record, path, value) match {
        case Success(actions) =>
          commit(id, actions, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case Insert(_, path, value) =>
      resetIdleTimeout()
      val commander = sender()
      avpath.insert(record, path, value) match {
        case Success(actions) =>
          commit(id, actions, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case InsertJson(_, path, value) =>
      resetIdleTimeout()
      val commander = sender()
      avpath.insertJson(record, path, value) match {
        case Success(actions) =>
          commit(id, actions, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case InsertAll(_, path, values) =>
      resetIdleTimeout()
      val commander = sender()
      avpath.insertAll(record, path, values) match {
        case Success(actions) =>
          commit(id, actions, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case InsertAllJson(_, path, values) =>
      resetIdleTimeout()
      val commander = sender()
      avpath.insertAllJson(record, path, values) match {
        case Success(actions) =>
          commit(id, actions, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case Delete(_, path) =>
      resetIdleTimeout()
      val commander = sender()
      avpath.delete(record, path) match {
        case Success(actions) =>
          commit(id, actions, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case Clear(_, path) =>
      resetIdleTimeout()
      val commander = sender()
      avpath.clear(record, path) match {
        case Success(actions) =>
          commit(id, actions, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

  }
}
