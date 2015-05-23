package chana.collection

import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import scala.reflect.ClassTag

/**
 *
 * @author Caoyuan Deng
 */
class ArrayListSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {

  "ArrayList" when {

    "issue SI-7268" should {
      "insert elements like" in {
        val si7268Tester = new SI7268Tester[Double]

        si7268Tester.insertAll(1.0)
        si7268Tester.values(0) should be(1.0)

        si7268Tester.insertOne(0.0)
        si7268Tester.values(0) should be(0.0)
        si7268Tester.values.size should be(2)

        //test.insertOk(1.0, 2.0)
        //test.insertOk(1.0)
        //test.insertFailed(1.0)
      }
    }

    var values: ArrayList[Int] = null

    "within maxCapacity" should {

      "append elements" in {
        values = new ArrayList[Int](3)

        info("====append 10 elements===============================")
        (0 to 9) foreach { i => values += i }
        infoValues(values)
        values.length should be(10)
        (0 to 9) foreach { i => values(i) should be(i) }
        var array = values.toArray
        (0 to 9) foreach { i => array(i) should be(i) }

        info("====append 2 more elements===============================")
        (10 to 11) foreach { i => values += i }
        infoValues(values)
        values.length should be(12)
        (0 to 11) foreach { i => values(i) should be(i) }
        array = values.toArray
        (0 to 11) foreach { i => array(i) should be(i) }

        info("====append 12 more elements===============================")
        (12 to 23) foreach { i => values += i }
        infoValues(values)
        values.length should be(24)
        (0 to 23) foreach { i => values(i) should be(i) }
        array = values.toArray
        (0 to 23) foreach { i => array(i) should be(i) }

        info("====batch append another 12 more elements===============================")
        values ++= (24 to 35)
        infoValues(values)
        values.length should be(36)
        (0 to 35) foreach { i => values(i) should be(i) }
        array = values.toArray
        (0 to 35) foreach { i => array(i) should be(i) }
      }
    }

    "with limited maxCapacity" should {

      "head/last" in {
        info("====original values")
        var values = new ArrayList[Int](3, 10)
        (0 to 12) foreach { i => values += i }
        infoValues(values)

        values.head should be(3)
        values.last should be(12)
      }

      "append elements" in {
        values = new ArrayList[Int](3, 10)

        info("====append 10 elements===============================")
        (0 to 9) foreach { i => values += i }
        infoValues(values)
        values.length should be(10)
        (0 to 9) foreach { i => values(i) should be(i) }
        var array = values.toArray
        (0 to 9) foreach { i => array(i) should be(i) }

        info("====append 2 more elements===============================")
        (10 to 11) foreach { i => values += i }
        infoValues(values)
        values.length should be(10)
        (0 to 9) foreach { i => values(i) should be(i + 2) }
        array = values.toArray
        (0 to 9) foreach { i => array(i) should be(i + 2) }

        info("====append 12 more elements===============================")
        (12 to 23) foreach { i => values += i }
        infoValues(values)
        values.length should be(10)
        (0 to 9) foreach { i => values(i) should be(i + 14) }
        array = values.toArray
        (0 to 9) foreach { i => array(i) should be(i + 14) }

        info("====batch append another 12 more elements===============================")
        values ++= (24 to 35)
        infoValues(values)
        values.length should be(10)
        (0 to 9) foreach { i => values(i) should be(i + 26) }
        array = values.toArray
        (0 to 9) foreach { i => array(i) should be(i + 26) }

        info("====batch append another 24 more elements===============================")
        values ++= (36 to 57)
        infoValues(values)
        values.length should be(10)
        (0 to 9) foreach { i => values(i) should be(i + 48) }
        array = values.toArray
        (0 to 9) foreach { i => array(i) should be(i + 48) }

        info("====batch append another 12 more elements===============================")
        values ++= (58 to 69)
        infoValues(values)
        values.length should be(10)
        (0 to 9) foreach { i => values(i) should be(i + 60) }
        array = values.toArray
        (0 to 9) foreach { i => array(i) should be(i + 60) }

        info("====batch append another 2 more elements===============================")
        values ++= (70 to 71)
        infoValues(values)
        values.length should be(10)
        (0 to 9) foreach { i => values(i) should be(i + 62) }
        array = values.toArray
        (0 to 9) foreach { i => array(i) should be(i + 62) }
      }

      "simple batch append" in {
        var values = new ArrayList[Int](3, 10)
        values ++= (0 to 5)
        infoValues(values)
        (0 to 5) foreach { i => values(i) should be(i) }
        var array = values.toArray
        (0 to 5) foreach { i => array(i) should be(i) }

        values = new ArrayList[Int](3, 10)
        values ++= (0 to 11)
        infoValues(values)
        (0 to 9) foreach { i => values(i) should be(i + 2) }
        array = values.toArray
        (0 to 9) foreach { i => array(i) should be(i + 2) }
      }

      "copyToArray" in {
        info("====copy when cursor is 0===============================")
        values = new ArrayList[Int](3, 10)
        values ++= (0 to 8)
        var dest = Array.ofDim[Int](8)
        values.copyToArray(dest, 0, 5)
        infoValues(values)
        info(dest.mkString(","))
        (0 to 4) foreach (i => dest(i) should be(i))

        info("====copy when cursor is > 0===============================")
        values ++= (9 to 11)
        dest = Array.ofDim[Int](10)
        values.copyToArray(dest, 0, 10)
        infoValues(values)
        info(dest.mkString(","))
        (0 to 9) foreach (i => dest(i) should be(i + 2))
      }

      "sliceToArray" in {
        info("====copy when cursor is 0===============================")
        values = new ArrayList[Int](3, 10)
        values ++= (0 to 8)
        var dest = values.sliceToArray(0, 5)
        infoValues(values)
        info(dest.mkString(","))
        (0 to 4) foreach (i => dest(i) should be(i))

        info("====copy when cursor is > 0===============================")
        values ++= (9 to 11)
        dest = values.sliceToArray(1, 10)
        infoValues(values)
        info(dest.mkString(","))
        (0 to 8) foreach (i => dest(i) should be(i + 3))
      }

      "insertOne when cursor is 0" in {
        info("====original values===============================")
        values = new ArrayList[Int](3, 10)
        (0 to 9).reverse foreach { i => values.insertOne(0, i) }
        infoValues(values)
        values.toArray should be(Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))

        info("====insert behind cursor===============================")
        values.insertOne(2, -1)
        infoValues(values)
        values.toArray should be(Array(1, 2, -1, 3, 4, 5, 6, 7, 8, 9))

        info("====original values===============================")
        values = new ArrayList[Int](3, 10)
        (0 to 9).reverse foreach { i => values.insertOne(0, i) }
        infoValues(values)
        values.toArray should be(Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))

        info("====insert at cursor===============================")
        values.insertOne(0, -1)
        infoValues(values)
        values.toArray should be(Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))

        info("====original values===============================")
        values = new ArrayList[Int](3, 10)
        (0 to 8).reverse foreach { i => values.insertOne(0, i) }
        infoValues(values)
        values.toArray should be(Array(0, 1, 2, 3, 4, 5, 6, 7, 8))

        info("====insert at size when not full (append actually)===============================")
        values.insertOne(9, -1)
        infoValues(values)
        values.toArray should be(Array(0, 1, 2, 3, 4, 5, 6, 7, 8, -1))

        info("====original values===============================")
        values = new ArrayList[Int](3, 10)
        (0 to 9).reverse foreach { i => values.insertOne(0, i) }
        infoValues(values)
        values.toArray should be(Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))

        info("====insert at size when full (append actually)===============================")
        values.insertOne(10, -1)
        infoValues(values)
        values.toArray should be(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, -1))
      }

      "insertOne when cursor is > 0" in {
        info("====original values===============================")
        values = new ArrayList[Int](3, 10)
        (0 to 12) foreach { i => values += i }
        infoValues(values)
        values.toArray should be(Array(3, 4, 5, 6, 7, 8, 9, 10, 11, 12))

        info("====insert behind cursor at 4===============================")
        values.insertOne(4, -1)
        infoValues(values)
        values.toArray should be(Array(4, 5, 6, 7, -1, 8, 9, 10, 11, 12))

        info("====original values===============================")
        values = new ArrayList[Int](3, 10)
        (0 to 12) foreach { i => values += i }
        infoValues(values)
        values.toArray should be(Array(3, 4, 5, 6, 7, 8, 9, 10, 11, 12))

        info("====insert before cursor at 1===============================")
        values.insertOne(1, -1)
        infoValues(values)
        values.toArray should be(Array(4, -1, 5, 6, 7, 8, 9, 10, 11, 12))

        info("====original values===============================")
        values = new ArrayList[Int](3, 10)
        (0 to 12) foreach { i => values += i }
        infoValues(values)
        values.toArray should be(Array(3, 4, 5, 6, 7, 8, 9, 10, 11, 12))

        info("====insert before cursor at 0===============================")
        values.insertOne(0, -1)
        infoValues(values)
        values.toArray should be(Array(3, 4, 5, 6, 7, 8, 9, 10, 11, 12))
      }

      "insertAll when cursor is 0" in {
        info("====original values===============================")
        values = new ArrayList[Int](3, 10)
        values.insertAll(0, List(0, 1, 2))
        infoValues(values)
        values.toArray should be(Array(0, 1, 2))

        info("====insert at 1===============================")
        values.insertAll(1, List(3, 4, 5))
        infoValues(values)
        values.toArray should be(Array(0, 3, 4, 5, 1, 2))

        info("====insert at 1 and exceed capacity===============================")
        values.insertAll(1, List(-1, -2, -3, -4, -5, -6))
        infoValues(values)
        values.toArray should be(Array(-2, -3, -4, -5, -6, 3, 4, 5, 1, 2))

        info("====original values===============================")
        values = new ArrayList[Int](3, 10)
        values.insertAll(0, List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
        infoValues(values)
        values.toArray should be(Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))

        info("====insert at 1 when full ===============================")
        values.insertAll(1, List(3, 4, 5))
        infoValues(values)
        values.toArray should be(Array(5, 1, 2, 3, 4, 5, 6, 7, 8, 9))
      }

      "insertAll when cursor is > 0" in {
        info("====original values===============================")
        values = new ArrayList[Int](3, 10)
        (0 to 12) foreach { i => values += i }
        infoValues(values)
        values.toArray should be(Array(3, 4, 5, 6, 7, 8, 9, 10, 11, 12))

        info("====insert before cursor0 at 1 ===============================")
        values.insertAll(1, List(-1, -2, -3))
        infoValues(values)
        values.toArray should be(Array(-3, 4, 5, 6, 7, 8, 9, 10, 11, 12))

        info("====original values===============================")
        values = new ArrayList[Int](3, 10)
        (0 to 12) foreach { i => values += i }
        infoValues(values)
        values.toArray should be(Array(3, 4, 5, 6, 7, 8, 9, 10, 11, 12))

        info("====insert after cursor0 at 4 ===============================")
        values.insertAll(4, List(-1, -2, -3))
        infoValues(values)
        values.toArray should be(Array(6, -1, -2, -3, 7, 8, 9, 10, 11, 12))
      }

      "reduceToSize when cursor is 0" in {
        info("====original values===============================")
        values = new ArrayList[Int](3, 10)
        (0 to 8) foreach { i => values += i }
        infoValues(values)
        values.toArray should be(Array(0, 1, 2, 3, 4, 5, 6, 7, 8))

        info("==== reduce to 0===============================")
        values.reduceToSize(0)
        infoValues(values)
        values.cursor0 should be(0)
        values.toArray should be(Array())

        info("====original values===============================")
        values = new ArrayList[Int](3, 10)
        (0 to 8) foreach { i => values += i }
        infoValues(values)
        values.toArray should be(Array(0, 1, 2, 3, 4, 5, 6, 7, 8))

        info("==== reduce to 6===============================")
        values.reduceToSize(6)
        infoValues(values)
        values.cursor0 should be(0)
        values.toArray should be(Array(0, 1, 2, 3, 4, 5))
      }

      "reduceToSize when cursor > 0" in {
        info("====original values===============================")
        values = new ArrayList[Int](3, 10)
        (0 to 12) foreach { i => values += i }
        infoValues(values)
        values.toArray should be(Array(3, 4, 5, 6, 7, 8, 9, 10, 11, 12))

        info("==== reduce when all behind cursor ===============================")
        values.reduceToSize(3)
        infoValues(values)
        values.cursor0 should be(0)
        values.toArray should be(Array(3, 4, 5))

        info("====original values===============================")
        values = new ArrayList[Int](3, 10)
        (0 to 12) foreach { i => values += i }
        infoValues(values)
        values.toArray should be(Array(3, 4, 5, 6, 7, 8, 9, 10, 11, 12))

        info("==== reduce when across cursor ===============================")
        values.reduceToSize(6)
        infoValues(values)
        values.cursor0 should be(0)
        values.toArray should be(Array(3, 4, 5, 6, 7, 8))
      }

      "remove when cursor > 0" in {
        info("====original values===============================")
        values = new ArrayList[Int](3, 10)
        (0 to 12) foreach { i => values += i }
        infoValues(values)
        values.cursor0 should be(3)
        values.toArray should be(Array(3, 4, 5, 6, 7, 8, 9, 10, 11, 12))

        info("==== remove behind cursor===============================")
        values.remove(0)
        infoValues(values)
        values.cursor0 should be(0)
        values.toArray should be(Array(4, 5, 6, 7, 8, 9, 10, 11, 12))

        info("====original values===============================")
        values = new ArrayList[Int](3, 10)
        (0 to 12) foreach { i => values += i }
        infoValues(values)
        values.cursor0 should be(3)
        values.toArray should be(Array(3, 4, 5, 6, 7, 8, 9, 10, 11, 12))

        info("==== remove across cursor ===============================")
        values.remove(1, 8)
        infoValues(values)
        values.cursor0 should be(0)
        values.toArray should be(Array(3, 12))

        info("====original values===============================")
        values = new ArrayList[Int](3, 10)
        (0 to 12) foreach { i => values += i }
        values.cursor0 should be(3)
        infoValues(values)
        values.toArray should be(Array(3, 4, 5, 6, 7, 8, 9, 10, 11, 12))

        info("==== remove before cursor ===============================")
        values.remove(8, 2)
        infoValues(values)
        values.cursor0 should be(0)
        values.toArray should be(Array(3, 4, 5, 6, 7, 8, 9, 10))
      }

      "reverse" in {
        info("====original values===============================")
        values = new ArrayList[Int](3, 10)
        (0 to 12) foreach { i => values += i }
        infoValues(values)
        values.cursor0 should be(3)
        values.toArray should be(Array(3, 4, 5, 6, 7, 8, 9, 10, 11, 12))

        info("==== remove across cursor ===============================")
        val reversed = values.reverse
        infoValues(reversed)
        reversed.toArray should be(Array(12, 11, 10, 9, 8, 7, 6, 5, 4, 3))
      }

    }

    def infoValues(values: ArrayList[Int]) {
      info(values.mkString(","))
      info("underlying array(cursor0=" + values.cursor0 + "):")
      info(values.array.mkString(","))
    }
  }

  /**
   * https://issues.scala-lang.org/browse/SI-7268
   */
  class SI7268Tester[V: ClassTag] {
    val values = new ArrayList[V]()

    def insertOne(v: V) {
      values.insertOne(0, v)
      info(values.mkString(","))
    }

    def insertAll(v: V) {
      values.insertAll(0, Array(v))
      info(values.mkString(","))
    }

    def insertOk(v: V) {
      val xs = Array(v)
      values.insert(0, xs: _*)
      info(values.mkString(","))
    }

    def insertOk(v1: V, v2: V) {
      val xs = Array(v1, v2)
      values.insert(0, xs: _*)
      info(values.mkString(","))
    }

    def insertFailed(v: V) {
      // v will be boxed to java.lang.Object (java.lang.Double) due to ClassTag, 
      // then wrapped as Array[Object] and passed to function in scala.LowPriorityImplicits:
      //    implicit def genericWrapArray[T](xs: Array[T]): WrappedArray[T]
      // for insert(n: Int, elems: A*), and causes ArrayStoreException.
      values.insert(0, v)
      info(values.mkString(","))
    }

    def insertFailed(v1: V, v2: V) {
      values.insert(0, v1, v2)
      info(values.mkString(","))
    }
  }

}
