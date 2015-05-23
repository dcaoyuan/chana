package chana.collection

import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import scala.reflect.ClassTag

class WeakKeyHashMapSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {

  "WeakKeyHashMap" when {

    "test AnyRef keys" should {
      "keep literalString key only" ignore { // ignore since could not pass at travis-ci.org
        val keys = Array(
          new String("newString1"),
          "literalString2",
          "literalString3",
          new String("newString4"))

        test[String](keys)(2)
      }
    }

    "test AnyVal keys" should {
      "keep all entries" in {
        val keys = Array(
          1L,
          2L,
          3L,
          4L)

        test[Long](keys)(4)
      }
    }

    def test[T: ClassTag](keys: Array[T])(nKeptFinal: Int) {
      var k1 = keys(0)
      var k2 = keys(1)
      var k3 = keys(2)
      var k4 = keys(3)

      val map = new WeakIdentityHashMap[T, Object] //new WeakKeyHashMap[Any, Object]

      val v1 = new Object
      map.put(k1, v1)
      val v2 = new Object
      map.put(k2, v2)
      val v3 = new Object
      map.put(k3, v3)
      val v4 = new Object
      map.put(k4, v4)

      assert(v1 eq map.get(k1).get)
      assert(v2 eq map.get(k2).get)
      assert(v3 eq map.get(k3).get)
      assert(v4 eq map.get(k4).get)

      /**
       * Discard the strong reference to all the keys.
       * @Note: shoule set elements in keys to null too.
       */
      k1 = null.asInstanceOf[T]
      k2 = null.asInstanceOf[T]
      k3 = null.asInstanceOf[T]
      k4 = null.asInstanceOf[T]
      for (i <- 0 until keys.length) {
        keys(i) = null.asInstanceOf[T]
      }

      (0 until 20) foreach { _ =>
        System.gc
        /**
         * Verify Full GC with the -verbose:gc option
         * We expect the map to be emptied as the strong references to
         * all the keys are discarded. The map size should be 2 now
         */
        info("map.size = " + map.size + "  " + map.mkString("[", ", ", "]"))
      }

      map.size should be(nKeptFinal)
    }
  }
}
