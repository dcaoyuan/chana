package chana.timeseries

import java.util.Calendar
import java.util.logging.Logger
import chana.collection.ArrayList
import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * @author Caoyuan Deng
 */
final class TQueue[V: ClassTag](freq: TFreq, limit: Int, onlyOneValue: Boolean = false) {

  private val log = Logger.getLogger(getClass.getName)
  private val lastIdx = limit - 1
  private var _time_xs = new Array[(Long, mutable.HashMap[String, Set[V]])](limit)

  val length = limit
  def apply(i: Int): (Long, collection.Map[String, Set[V]]) = _time_xs synchronized { _time_xs(i) }
  def last() = apply(lastIdx)

  def put(key: String, time: Long, value: V): Unit = _time_xs synchronized {
    var willAdd = true
    var lastKeyToXs = _time_xs(lastIdx) match {
      case null => null
      case (timeMax, keyToXs) =>
        val nFreqs = freq.nFreqsBetween(timeMax, time)
        if (nFreqs == 0) {
          keyToXs
        } else if (nFreqs > 0) {
          val newTime_xs = new Array[(Long, mutable.HashMap[String, Set[V]])](limit)
          System.arraycopy(_time_xs, 1, newTime_xs, 0, lastIdx)
          _time_xs = newTime_xs
          _time_xs(lastIdx) = null
          null
        } else {
          willAdd = false
          null
        }
    }

    if (willAdd) {
      if (lastKeyToXs == null) {
        val newXs = new ArrayList[V]
        newXs += value
        val newMap = mutable.HashMap[String, Set[V]](key -> newXs.toSet)
        _time_xs(lastIdx) = (time, newMap)
      } else {
        var xs = lastKeyToXs.get(key).getOrElse(new ArrayList[V]().toSet)
        xs += value
        if (onlyOneValue) xs = Array(value).toSet
        lastKeyToXs(key) = xs
      }
    }
  }
}

object TQueue {
  // --- simple test
  def main(args: Array[String]) {
    val tq = new TQueue[Int](TFreq.DAILY, 2)

    val cal = Calendar.getInstance
    cal.setTimeInMillis(0)
    cal.set(1990, 0, 1)
    for (i <- 0 until 10) {
      cal.add(Calendar.DAY_OF_MONTH, 1)
      tq.put("a", cal.getTimeInMillis, i)
      tq.put("a", cal.getTimeInMillis, i + 10)
      tq.put("b", cal.getTimeInMillis, i)
      tq.put("b", cal.getTimeInMillis, i + 10)
    }

    println(tq._time_xs(0))
    println(tq._time_xs(1))
    val ok = {
      tq._time_xs(0) == (631929600000L, Map("a" -> Set(8), "b" -> Set(8))) &&
        tq._time_xs(1) == (632016000000L, Map("a" -> Set(9), "b" -> Set(9)))
    }
    println(ok)
    assert(ok, "Error.")
  }
}

