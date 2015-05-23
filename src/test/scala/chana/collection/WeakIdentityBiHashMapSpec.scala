package chana.collection

import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.WordSpecLike

class WeakIdentityBiHashMapSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {

  "WeakIdentityBiHashMap" when {

    "Non strongly refered literal String / AnyRef String keys" should {
      "be cleared" in {

        var keys = Array(
          new String("newString1"),
          "literalString2",
          "literalString3",
          new String("newString4"),
          new String("newString5"))

        var k1 = keys(0)
        var k2 = keys(1)
        var k3 = keys(2)
        var k4 = keys(3)
        var k5 = keys(4)

        val map = new WeakIdentityBiHashMap[String, Long]

        val v1 = 1
        map.put(k1, v1)
        val v2 = 2
        map.put(k2, v2)
        val v3 = 3
        map.put(k3, v3)
        val v4 = 4
        map.put(k4, v4)

        map.get(k1) should be(Some(v1))
        map.get(k2) should be(Some(v2))
        map.get(k3) should be(Some(v3))
        map.get(k4) should be(Some(v4))

        map.getByValue(v1) should be(Some(k1))
        map.getByValue(v2) should be(Some(k2))
        map.getByValue(v3) should be(Some(k3))
        map.getByValue(v4) should be(Some(k4))

        // this put should remove k4, the pair is now: k5 <-> v4
        map.put(k5, v4)

        map.get(k4) should be(None)
        map.getByValue(v4) should be(Some(k5))
        map.get(k5) should be(Some(v4))

        /**
         * Discard the strong reference to all keys.
         * @Note: shoule set elements in keys to null too.
         */
        k1 = null.asInstanceOf[String]
        k2 = null.asInstanceOf[String]
        k3 = null.asInstanceOf[String]
        k4 = null.asInstanceOf[String]
        k5 = null.asInstanceOf[String]
        for (i <- 0 until keys.length) {
          keys(i) = null.asInstanceOf[String]
        }

        (0 until 20) foreach { _ =>
          System.gc
          /**
           * Verify Full GC with the -verbose:gc option
           * We expect the map to be cleared for those keys that are not strongly refered
           * The map size should be 2 now
           */
          info("map.size should be 2 after a while: " + map.size + "  " + map.mkString("[", ", ", "]"))
          info("key by value 1 should be None: " + map.getByValue(1))
          info("key by value 2 should be Some: " + map.getByValue(2))
          info("key by value 3 should be Some: " + map.getByValue(3))
          info("key by value 4 should be None: " + map.getByValue(4))
        }
        map.getByValue(1) should be(None)
        map.getByValue(2) should be(Some("literalString2"))
        map.getByValue(3) should be(Some("literalString3"))
        map.getByValue(4) should be(None)
        map.size should be(2)
      }
    }

    "Non strongly refered map for lots keys" should {
      "be cleared after a while" in {
        val size = 10000
        val map = new WeakIdentityBiHashMap[String, Long]
        var keys = for (i <- 0 until size) yield {
          val k = new String("String" + i)
          map.put(k, i)
          k
        }

        for (i <- 0 until size) {
          //println(i)
          assert(map.getByValue(i).get == ("String" + i), "Value " + i + " lost entry")
          assert(map.get("String" + i).isEmpty, "Identity Map error at " + i + ": new String does not identity equal")
          assert(map.get(keys(i)).get == i, "Key " + keys(i) + " value should be: " + i + ", but it's: " + map.get(keys(i)))
        }

        keys = null
        //map foreach println

        (0 until 20) foreach { _ =>
          System.gc
          /**
           * Verify Full GC with the -verbose:gc option
           * We expect the map to be cleared for those keys that are not strongly refered
           * The map size should be 0 now
           */
          info("map.size should be 0 after a while: " + map.size + "  " + map.mkString("[", ", ", "]"))
        }

        map.size should be(0)
      }
    }
  }

}
