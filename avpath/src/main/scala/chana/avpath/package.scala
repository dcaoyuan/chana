package chana

import chana.avpath.Evaluator.Ctx
import chana.avro.UpdateAction
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

  def update(data: IndexedRecord, path: String, value: Any): Try[List[UpdateAction]] = update(new Parser())(data, path, value)
  def update(parser: Parser)(data: IndexedRecord, path: String, value: Any): Try[List[UpdateAction]] = {
    try {
      val ast = parser.parse(path)
      val actions = Evaluator.update(data, ast, value)
      Success(actions)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

  def updateJson(data: IndexedRecord, path: String, value: String): Try[List[UpdateAction]] = updateJson(new Parser())(data, path, value)
  def updateJson(parser: Parser)(data: IndexedRecord, path: String, value: String): Try[List[UpdateAction]] = {
    try {
      val ast = parser.parse(path)
      val actions = Evaluator.updateJson(data, ast, value)
      Success(actions)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

  /**
   * Applied on array/map only
   */
  def insert(data: IndexedRecord, path: String, value: Any): Try[List[UpdateAction]] = insert(new Parser())(data, path, value)
  def insert(parser: Parser)(data: IndexedRecord, path: String, value: Any): Try[List[UpdateAction]] = {
    try {
      val ast = parser.parse(path)
      val actions = Evaluator.insert(data, ast, value)
      Success(actions)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

  /**
   * Applied on array/map only
   */
  def insertJson(data: IndexedRecord, path: String, value: String): Try[List[UpdateAction]] = insertJson(new Parser())(data, path, value)
  def insertJson(parser: Parser)(data: IndexedRecord, path: String, value: String): Try[List[UpdateAction]] = {
    try {
      val ast = parser.parse(path)
      val actions = Evaluator.insertJson(data, ast, value)
      Success(actions)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

  /**
   * Applied on array/map only
   */
  def insertAll(data: IndexedRecord, path: String, values: java.util.Collection[_]): Try[List[UpdateAction]] = insertAll(new Parser())(data, path, values)
  def insertAll(parser: Parser)(data: IndexedRecord, path: String, values: java.util.Collection[_]): Try[List[UpdateAction]] = {
    try {
      val ast = parser.parse(path)
      val actions = Evaluator.insertAll(data, ast, values)
      Success(actions)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

  /**
   * Applied on array/map only
   */
  def insertAllJson(data: IndexedRecord, path: String, value: String): Try[List[UpdateAction]] = insertAllJson(new Parser())(data, path, value)
  def insertAllJson(parser: Parser)(data: IndexedRecord, path: String, value: String): Try[List[UpdateAction]] = {
    try {
      val ast = parser.parse(path)
      val actions = Evaluator.insertAllJson(data, ast, value)
      Success(actions)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

  /**
   * Applied on array/map elements only
   */
  def delete(data: IndexedRecord, path: String): Try[List[UpdateAction]] = delete(new Parser())(data, path)
  def delete(parser: Parser)(data: IndexedRecord, path: String): Try[List[UpdateAction]] = {
    try {
      val ast = parser.parse(path)
      val actions = Evaluator.delete(data, ast)
      Success(actions)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

  /**
   * Applied on array/map only
   */
  def clear(data: IndexedRecord, path: String): Try[List[UpdateAction]] = clear(new Parser())(data, path)
  def clear(parser: Parser)(data: IndexedRecord, path: String): Try[List[UpdateAction]] = {
    try {
      val ast = parser.parse(path)
      val actions = Evaluator.clear(data, ast)
      Success(actions)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

}
