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

  def updateAvro(data: IndexedRecord, path: String, value: Array[Byte]): Try[List[UpdateAction]] = updateAvro(parser())(data, path, value)
  def updateAvro(parser: XPathParser)(data: IndexedRecord, path: String, value: Array[Byte]): Try[List[UpdateAction]] = {
    Try {
      val ast = parser.parse(path)
      XPathEvaluator.updateAvro(data, ast, value)
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
  def insertAvro(data: IndexedRecord, path: String, value: Array[Byte]): Try[List[UpdateAction]] = insertAvro(parser())(data, path, value)
  def insertAvro(parser: XPathParser)(data: IndexedRecord, path: String, value: Array[Byte]): Try[List[UpdateAction]] = {
    Try {
      val ast = parser.parse(path)
      XPathEvaluator.insertAvro(data, ast, value)
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
   * Applied on array/map only
   */
  def insertAllAvro(data: IndexedRecord, path: String, value: Array[Byte]): Try[List[UpdateAction]] = insertAllAvro(parser())(data, path, value)
  def insertAllAvro(parser: XPathParser)(data: IndexedRecord, path: String, value: Array[Byte]): Try[List[UpdateAction]] = {
    Try {
      val ast = parser.parse(path)
      XPathEvaluator.insertAllAvro(data, ast, value)
    }
  }

  /**
   * Applied on array/map elements only
   */
  def delete(data: IndexedRecord, path: String): Try[List[UpdateAction]] = delete(parser())(data, path, java.util.Collections.emptyList())
  def delete(data: IndexedRecord, path: String, keys: java.util.Collection[_]): Try[List[UpdateAction]] = delete(parser())(data, path, keys)
  def delete(parser: XPathParser)(data: IndexedRecord, path: String, keys: java.util.Collection[_]): Try[List[UpdateAction]] = {
    Try {
      val xpath = if (keys.isEmpty) {
        path
      } else {
        val itr = keys.iterator
        val first = itr.next
        val sb = new StringBuilder(path)
        first match {
          case x: String =>
            sb.append("/@*[").append("name()='" + first + "'")

            while (itr.hasNext) {
              sb.append(" or name()='" + itr.next + "'")
            }
            sb.append("]")
          case x: Int =>
            sb.append("[").append("position()=" + first)

            while (itr.hasNext) {
              sb.append(" or position()=" + itr.next)
            }
            sb.append("]")
        }
        sb.toString
      }
      val ast = parser.parse(xpath)
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
