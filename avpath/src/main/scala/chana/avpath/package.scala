package chana

import chana.avpath.Evaluator.Ctx
import org.apache.avro.generic.IndexedRecord
import scala.util.Failure
import scala.util.Success
import scala.util.Try

package object avpath {

  def select(data: IndexedRecord, path: String): Try[List[Ctx]] = select(new Parser())(data, path)
  def select(parser: Parser)(data: IndexedRecord, path: String): Try[List[Ctx]] = {
    try {
      val ast = parser.parse(path)
      val ctxs = Evaluator.select(data, ast)
      Success(ctxs)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

  def update(data: IndexedRecord, path: String, value: Any): Try[List[Ctx]] = update(new Parser())(data, path, value)
  def update(parser: Parser)(data: IndexedRecord, path: String, value: Any): Try[List[Ctx]] = {
    try {
      val ast = parser.parse(path)
      val ctxs = Evaluator.update(data, ast, value)
      Success(ctxs)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

  def updateJson(data: IndexedRecord, path: String, value: String): Try[List[Ctx]] = updateJson(new Parser())(data, path, value)
  def updateJson(parser: Parser)(data: IndexedRecord, path: String, value: String): Try[List[Ctx]] = {
    try {
      val ast = parser.parse(path)
      val ctxs = Evaluator.updateJson(data, ast, value)
      Success(ctxs)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

  /**
   * Applied on array/map only
   */
  def insert(data: IndexedRecord, path: String, value: Any): Try[List[Ctx]] = insert(new Parser())(data, path, value)
  def insert(parser: Parser)(data: IndexedRecord, path: String, value: Any): Try[List[Ctx]] = {
    try {
      val ast = parser.parse(path)
      val ctxs = Evaluator.insert(data, ast, value)
      Success(ctxs)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

  /**
   * Applied on array/map only
   */
  def insertJson(data: IndexedRecord, path: String, value: String): Try[List[Ctx]] = insertJson(new Parser())(data, path, value)
  def insertJson(parser: Parser)(data: IndexedRecord, path: String, value: String): Try[List[Ctx]] = {
    try {
      val ast = parser.parse(path)
      val ctxs = Evaluator.insertJson(data, ast, value)
      Success(ctxs)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

  /**
   * Applied on array/map only
   */
  def insertAll(data: IndexedRecord, path: String, values: java.util.Collection[_]): Try[List[Ctx]] = insertAll(new Parser())(data, path, values)
  def insertAll(parser: Parser)(data: IndexedRecord, path: String, values: java.util.Collection[_]): Try[List[Ctx]] = {
    try {
      val ast = parser.parse(path)
      val ctxs = Evaluator.insertAll(data, ast, values)
      Success(ctxs)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

  /**
   * Applied on array/map only
   */
  def insertAllJson(data: IndexedRecord, path: String, value: String): Try[List[Ctx]] = insertAllJson(new Parser())(data, path, value)
  def insertAllJson(parser: Parser)(data: IndexedRecord, path: String, value: String): Try[List[Ctx]] = {
    try {
      val ast = parser.parse(path)
      val ctxs = Evaluator.insertAllJson(data, ast, value)
      Success(ctxs)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

  /**
   * Applied on array/map elements only
   */
  def delete(data: IndexedRecord, path: String): Try[List[Ctx]] = delete(new Parser())(data, path)
  def delete(parser: Parser)(data: IndexedRecord, path: String): Try[List[Ctx]] = {
    try {
      val ast = parser.parse(path)
      val ctxs = Evaluator.delete(data, ast)
      Success(ctxs)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

  /**
   * Applied on array/map only
   */
  def clear(data: IndexedRecord, path: String): Try[List[Ctx]] = clear(new Parser())(data, path)
  def clear(parser: Parser)(data: IndexedRecord, path: String): Try[List[Ctx]] = {
    try {
      val ast = parser.parse(path)
      val ctxs = Evaluator.clear(data, ast)
      Success(ctxs)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

}
