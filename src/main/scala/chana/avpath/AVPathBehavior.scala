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
      val toBe = new GenericData.Record(record, true)
      avpath.update(parser)(toBe, path, value) match {
        case Success(ctxs) =>
          commit(id, toBe, ctxs, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case UpdateJson(_, path, value) =>
      resetIdleTimeout()
      val commander = sender()
      val toBe = new GenericData.Record(record, true)
      avpath.updateJson(parser)(toBe, path, value) match {
        case Success(ctxs) =>
          commit(id, toBe, ctxs, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case Insert(_, path, value) =>
      resetIdleTimeout()
      val commander = sender()
      val toBe = new GenericData.Record(record, true)
      avpath.insert(parser)(toBe, path, value) match {
        case Success(ctxs) =>
          commit(id, toBe, ctxs, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case InsertJson(_, path, value) =>
      resetIdleTimeout()
      val commander = sender()
      val toBe = new GenericData.Record(record, true)
      avpath.insertJson(parser)(toBe, path, value) match {
        case Success(ctxs) =>
          commit(id, toBe, ctxs, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case InsertAll(_, path, values) =>
      resetIdleTimeout()
      val commander = sender()
      val toBe = new GenericData.Record(record, true)
      avpath.insertAll(parser)(toBe, path, values) match {
        case Success(ctxs) =>
          commit(id, toBe, ctxs, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case InsertAllJson(_, path, values) =>
      resetIdleTimeout()
      val commander = sender()
      val toBe = new GenericData.Record(record, true)
      avpath.insertAllJson(parser)(toBe, path, values) match {
        case Success(ctxs) =>
          commit(id, toBe, ctxs, commander)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case Delete(_, path) =>
      resetIdleTimeout()
      val commander = sender()
      val toBe = new GenericData.Record(record, true)
      avpath.delete(parser)(toBe, path) match {
        case Success(ctxs) =>
          commit(id, toBe, ctxs, commander, false)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

    case Clear(_, path) =>
      resetIdleTimeout()
      val commander = sender()
      val toBe = new GenericData.Record(record, true)
      avpath.clear(parser)(toBe, path) match {
        case Success(ctxs) =>
          commit(id, toBe, ctxs, commander, false)
        case x @ Failure(ex) =>
          log.error(ex, ex.getMessage)
          commander ! x
      }

  }
}
