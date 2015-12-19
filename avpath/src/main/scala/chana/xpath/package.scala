package chana

import chana.avro.UpdateAction
import chana.xpath.Context
import chana.xpath.nodes.XPathParser
import org.apache.avro.generic.IndexedRecord
import scala.util.Try

package object xpath {
  def parser() = new XPathParser()

  def select(data: IndexedRecord, path: String): Try[List[Context]] = select(parser())(data, path)
  def select(parser: XPathParser)(data: IndexedRecord, path: String): Try[List[Context]] = {
    Try {
      val ast = parser.parse(path)
      XPathEvaluator.select(data, ast)
    }
  }

  def update(data: IndexedRecord, path: String, value: Any): Try[List[UpdateAction]] = update(parser())(data, path, value)
  def update(parser: XPathParser)(data: IndexedRecord, path: String, value: Any): Try[List[UpdateAction]] = {
    Try {
      val ast = parser.parse(path)
      XPathEvaluator.update(data, ast, value)
    }
  }

  def updateJson(data: IndexedRecord, path: String, value: String): Try[List[UpdateAction]] = updateJson(parser())(data, path, value)
  def updateJson(parser: XPathParser)(data: IndexedRecord, path: String, value: String): Try[List[UpdateAction]] = {
    Try {
      val ast = parser.parse(path)
      XPathEvaluator.updateJson(data, ast, value)
    }
  }

  /**
   * Applied on array/map only
   */
  def insert(data: IndexedRecord, path: String, value: Any): Try[List[UpdateAction]] = insert(parser())(data, path, value)
  def insert(parser: XPathParser)(data: IndexedRecord, path: String, value: Any): Try[List[UpdateAction]] = {
    Try {
      val ast = parser.parse(path)
      XPathEvaluator.insert(data, ast, value)
    }
  }

  /**
   * Applied on array/map only
   */
  def insertJson(data: IndexedRecord, path: String, value: String): Try[List[UpdateAction]] = insertJson(parser())(data, path, value)
  def insertJson(parser: XPathParser)(data: IndexedRecord, path: String, value: String): Try[List[UpdateAction]] = {
    Try {
      val ast = parser.parse(path)
      XPathEvaluator.insertJson(data, ast, value)
    }
  }

  /**
   * Applied on array/map only
   */
  def insertAll(data: IndexedRecord, path: String, values: java.util.Collection[_]): Try[List[UpdateAction]] = insertAll(parser())(data, path, values)
  def insertAll(parser: XPathParser)(data: IndexedRecord, path: String, values: java.util.Collection[_]): Try[List[UpdateAction]] = {
    Try {
      val ast = parser.parse(path)
      XPathEvaluator.insertAll(data, ast, values)
    }
  }

  /**
   * Applied on array/map only
   */
  def insertAllJson(data: IndexedRecord, path: String, value: String): Try[List[UpdateAction]] = insertAllJson(parser())(data, path, value)
  def insertAllJson(parser: XPathParser)(data: IndexedRecord, path: String, value: String): Try[List[UpdateAction]] = {
    Try {
      val ast = parser.parse(path)
      XPathEvaluator.insertAllJson(data, ast, value)
    }
  }

  /**
   * Applied on array/map elements only
   */
  def delete(data: IndexedRecord, path: String): Try[List[UpdateAction]] = delete(parser())(data, path)
  def delete(parser: XPathParser)(data: IndexedRecord, path: String): Try[List[UpdateAction]] = {
    Try {
      val ast = parser.parse(path)
      XPathEvaluator.delete(data, ast)
    }
  }

  /**
   * Applied on array/map only
   */
  def clear(data: IndexedRecord, path: String): Try[List[UpdateAction]] = clear(parser())(data, path)
  def clear(parser: XPathParser)(data: IndexedRecord, path: String): Try[List[UpdateAction]] = {
    Try {
      val ast = parser.parse(path)
      XPathEvaluator.clear(data, ast)
    }
  }

}
