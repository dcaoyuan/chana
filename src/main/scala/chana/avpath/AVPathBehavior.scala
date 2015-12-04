package chana.avpath

import chana.Command
import chana.Entity
import chana.avpath
import chana.avpath.Evaluator.Ctx
import org.apache.avro.generic.GenericData
import scala.util.Failure
import scala.util.Success
import scala.util.Try

final case class Select(id: String, path: String) extends Command
final case class SelectAvro(id: String, path: String) extends Command
final case class SelectJson(id: String, path: String) extends Command
final case class Update(id: String, path: String, value: Any) extends Command
final case class UpdateJson(id: String, path: String, value: String) extends Command
final case class Insert(id: String, path: String, value: Any) extends Command
final case class InsertJson(id: String, path: String, value: String) extends Command
final case class InsertAll(id: String, path: String, values: java.util.Collection[_]) extends Command
final case class InsertAllJson(id: String, path: String, values: String) extends Command
final case class Delete(id: String, path: String) extends Command
final case class Clear(id: String, path: String) extends Command

trait AVPathBehavior extends Entity {
  protected def parser = new avpath.Parser()

  def avpathBehavior: Receive = {
    case Select(_, path) =>
      resetIdleTimeout()
      val commander = sender()
      avpath.select(parser)(record, path) match {
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
      avpath.select(parser)(record, path) match {
        case x @ Success(ctxs) =>
          Try(ctxs.map { ctx => encoderDecoder.avroEncode(ctx.value, ctx.schema).get }) match {
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
      avpath.select(parser)(record, path) match {
        case Success(ctxs) =>
          Try(ctxs.map { ctx => encoderDecoder.jsonEncode(ctx.value, ctx.schema).get }) match {
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
      avpath.update(parser)(record, path, value) match {
        case Success(actions) =>
          commit(id, actions, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case UpdateJson(_, path, value) =>
      resetIdleTimeout()
      val commander = sender()
      avpath.updateJson(parser)(record, path, value) match {
        case Success(actions) =>
          commit(id, actions, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case Insert(_, path, value) =>
      resetIdleTimeout()
      val commander = sender()
      avpath.insert(parser)(record, path, value) match {
        case Success(actions) =>
          commit(id, actions, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case InsertJson(_, path, value) =>
      resetIdleTimeout()
      val commander = sender()
      avpath.insertJson(parser)(record, path, value) match {
        case Success(actions) =>
          commit(id, actions, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case InsertAll(_, path, values) =>
      resetIdleTimeout()
      val commander = sender()
      avpath.insertAll(parser)(record, path, values) match {
        case Success(actions) =>
          commit(id, actions, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case InsertAllJson(_, path, values) =>
      resetIdleTimeout()
      val commander = sender()
      avpath.insertAllJson(parser)(record, path, values) match {
        case Success(actions) =>
          commit(id, actions, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case Delete(_, path) =>
      resetIdleTimeout()
      val commander = sender()
      avpath.delete(parser)(record, path) match {
        case Success(actions) =>
          commit(id, actions, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case Clear(_, path) =>
      resetIdleTimeout()
      val commander = sender()
      avpath.clear(parser)(record, path) match {
        case Success(actions) =>
          commit(id, actions, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

  }
}
