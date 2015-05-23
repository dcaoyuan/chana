package chana.timeseries

import java.util.Calendar
import java.util.TimeZone

/**
 *
 * @author Caoyuan Deng
 */
object TSerMerger {

  def merge(srcSer: TBaseSer, tarSer: TBaseSer, timeZone: TimeZone, fromTime: Long) {
    val tvalShift = new TValShift(tarSer.freq, Calendar.getInstance(timeZone))
    var prevTVal: TVal = null

    val cal = Calendar.getInstance(timeZone)
    cal.setTimeInMillis(fromTime)
    val roundedFromTime = tarSer.freq.round(fromTime, cal)
    val srcFromIdx = math.max(0, srcSer.timestamps.indexOrNextIndexOfOccurredTime(roundedFromTime))

    val n = srcSer.size
    var i = srcFromIdx
    var srcTime = 0L
    while (i < n && { srcTime = srcSer.timeOfIndex(i); srcTime >= roundedFromTime }) {
      val tval = tvalShift.valOf(srcTime)
      val tarTime = tval.time

      val srcVars = srcSer.vars.iterator
      val tarVars = tarSer.vars.iterator

      if (tarSer.nonExists(tarTime)) {
        tarSer.createOrReset(tarTime)

        while (srcVars.hasNext && tarVars.hasNext) { // srcVars.size and order should be same as tarVars
          val srcVar = srcVars.next
          val tarVar = tarVars.next

          tarVar.castingUpdate(tarTime, srcVar(srcTime))
        }

      } else {

        while (srcVars.hasNext && tarVars.hasNext) { // srcVars.size and order should be same as tarVars
          val srcVar = srcVars.next
          val tarVar = tarVars.next

          tarVar.kind match {
            case TVar.Kind.Open      => // do nothing
            case TVar.Kind.High      => tarVar.castingUpdate(tarTime, math.max(tarVar.double(tarTime), srcVar.double(srcTime)))
            case TVar.Kind.Low       => tarVar.castingUpdate(tarTime, math.min(tarVar.double(tarTime), srcVar.double(srcTime)))
            case TVar.Kind.Close     => tarVar.castingUpdate(tarTime, srcVar(srcTime))
            case TVar.Kind.Accumlate => tarVar.castingUpdate(tarTime, tarVar.double(tarTime) + srcVar.double(srcTime))
          }
        }

      }

      if (tval.justOpen_?) {
        tval.unjustOpen_!
      }

      if (prevTVal != null && prevTVal.closed_?) {
        // prevTval closed
      }

      prevTVal = tval
      i += 1
    }
  }

  /**
   * This function keeps the adjusting linear according to a norm
   */
  private def linearAdjust(value: Double, prevNorm: Double, postNorm: Double): Double = {
    ((value - prevNorm) / prevNorm) * postNorm + postNorm
  }

}

