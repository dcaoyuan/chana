package chana.timeseries

import akka.pattern.ask
import chana.Thing
import chana.timeseries.indicator.Factor
import chana.timeseries.indicator.Function
import chana.timeseries.indicator.Id
import chana.timeseries.indicator.Indicator
import chana.util.Reflect
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration._
import scala.reflect.ClassTag

/**
 *
 * @author Caoyuan Deng
 */
class DefaultTBaseSer(
    _thing: Thing,
    _freq: TFreq = TFreq.DAILY,
    initialSize: Int = 72,
    maxCapacity: Int = 20160) extends DefaultTSer(_freq, initialSize, maxCapacity) with TBaseSer {

  private var _isOnCalendarMode = false
  private lazy val idToFunction = new ConcurrentHashMap[Id[_ <: Function], Function](8, 0.9f, 1)
  private lazy val idToIndicator = new ConcurrentHashMap[Id[_ <: Indicator], Indicator](8, 0.9f, 1)

  attach(TStamps(initialSize, maxCapacity))

  def context = thing.context

  def function[T <: Function: ClassTag](functionClass: Class[T], args: Any*): T = {
    val id = Id(functionClass, this, args: _*)
    idToFunction.get(id) match {
      case null =>
        /** if got none from idToFunction, try to create new one */
        try {
          val function = Reflect.instantiate(functionClass, this :: args.toList)
          idToFunction.putIfAbsent(id, function)
          function
        } catch {
          case ex: Throwable => log.error(ex, ex.getMessage); null.asInstanceOf[T]
        }
      case x => x.asInstanceOf[T]
    }
  }

  def indicator[T <: Indicator: ClassTag](indicatorClass: Class[T], factors: Factor*): T = {
    val id = Id(indicatorClass, this, factors)
    idToIndicator.get(id) match {
      case null =>
        /** if got none from idToFunction, try to create new one */
        try {
          val indicator = Reflect.instantiate(indicatorClass, this :: factors.toList)
          //indicator.factors = factors.toArray // set factors first to avoid multiple computeFrom(0)
          /** don't forget to call set(baseSer) immediatley */
          idToIndicator.putIfAbsent(id, indicator)
          indicator.computeFrom(0)
          indicator
        } catch {
          case ex: Throwable => log.error(ex, ex.getMessage); null.asInstanceOf[T]
        }
      case x => x.asInstanceOf[T]
    }
  }

  def thing = _thing

  /*-
 * !NOTICE
 * This should be the only place to create an Item from outside, because it's
 * a bit complex to finish an item creating procedure, the procedure contains
 * at least 3 steps:
 * 1. create a clear holder, which with clear = true, and idx to be set
 *    later by holders;
 * 2. add the time to timestamps properly.
 * @see #internal_addClearItemAndNullVarValuesToList_And_Filltimestamps__InTimeOrder(long, SerItem)
 * 3. add null value to vars at the proper idx.
 * @see #internal_addTime_addClearItem_addNullVarValues()
 *
 * So we do not try to provide other public methods such as addItem() that can
 * add item from outside, you should use this method to create a new (a clear)
 * item and return it, or just clear it, if it has be there.
 * And that why define some motheds signature begin with internal_, becuase
 * you'd better never think to open these methods to protected or public.
 * @return Returns the index of time.
 */
  def createOrReset(time: Long) {
    try {
      writeLock.lock
      /**
       * @NOTE:
       * Should only get index from timestamps which has the proper
       * position <-> time <-> item mapping
       */
      val idx = timestamps.indexOfOccurredTime(time)
      if (idx >= 0 && idx < holders.size) {
        // existed, reset it
        vars foreach (_.reset(idx))
        holders(idx) = false
      } else {
        // append at the end: create a new one, add placeholder
        internal_addItem_fillTimestamps_inTimeOrder(time, true)
      }

    } finally {
      writeLock.unlock
    }
  }

  def createWhenNonExist(time: Long) {
    try {
      writeLock.lock
      /**
       * @NOTE:
       * Should only get index from timestamps which has the proper
       * position <-> time <-> item mapping
       */
      val idx = timestamps.indexOfOccurredTime(time)
      if (!(idx >= 0 && idx < holders.size)) {
        // append at the end: create a new one, add placeholder
        internal_addItem_fillTimestamps_inTimeOrder(time, true)
      }

    } finally {
      writeLock.unlock
    }
  }

  /**
   * Add a null item and corresponding time in time order,
   * should process time position (add time to timestamps orderly).
   * Support inserting time/clearItem pair in random order
   *
   * @param time
   * @param clearItem
   */
  private def internal_addItem_fillTimestamps_inTimeOrder(time: Long, holder: Holder): Int = {
    // @Note: writeLock timestamps only when insert/append it
    val lastOccurredTime = timestamps.lastOccurredTime
    if (time < lastOccurredTime) {
      val existIdx = timestamps.indexOfOccurredTime(time)
      if (existIdx >= 0) {
        vars foreach (_.putNull(time))
        // as timestamps includes this time, we just always put in a none-null item
        holders.insertOne(existIdx, holder)

        existIdx
      } else {
        val idx = timestamps.indexOrNextIndexOfOccurredTime(time)
        assert(idx >= 0, "Since itemTime < lastOccurredTime, the idx=" + idx + " should be >= 0")

        // (time at idx) > itemTime, insert this new item at the same idx, so the followed elems will be pushed behind
        try {
          timestamps.writeLock.lock

          // should add timestamps first
          timestamps.insertOne(idx, time)
          timestamps.log.logInsert(1, idx)

          vars foreach (_.putNull(time))
          holders.insertOne(idx, holder)

          idx

          // @todo Not remove it now
          //          if (timestamps.size > MAX_DATA_SIZE){
          //            val length = timestamps.size - MAX_DATA_SIZE
          //            clearUntilIdx(length)
          //          }

        } finally {
          timestamps.writeLock.unlock
        }
      }
    } else if (time > lastOccurredTime) {
      // time > lastOccurredTime, just append it behind the last:
      try {
        timestamps.writeLock.lock

        // should append timestamps first
        timestamps += time
        timestamps.log.logAppend(1)

        vars foreach (_.putNull(time))
        holders += holder

        this.size - 1

        // @todo Not remove it now.
        //        if (timestamps.size > MAX_DATA_SIZE){
        //          val length = timestamps.size - MAX_DATA_SIZE
        //          clearUntilIdx(length)
        //        }

      } finally {
        timestamps.writeLock.unlock
      }
    } else {
      // time == lastOccurredTime, keep same time and append vars and holders.
      val existIdx = timestamps.indexOfOccurredTime(time)
      if (existIdx >= 0) {
        vars foreach (_.putNull(time))
        holders += holder

        size - 1
      } else {
        assert(
          false,
          "As it's an adding action, we should not reach here! " +
            "Check your code, you are probably from createOrReset(long), " +
            "Does timestamps.indexOfOccurredTime(itemTime) = " + timestamps.indexOfOccurredTime(time) +
            " return -1 ?")
        -1
        // to avoid concurrent conflict, just do nothing here.
      }
    }
  }

  private def clearUntilIdx(idx: Int) {
    timestamps.remove(0, idx)
    holders.remove(0, idx)
  }

  /**
   * Append TVals to ser.
   * To use this method, should define proper assignValue(value)
   */
  override def ++=[V <: TVal](values: Array[V]): TSer = {
    if (values.length == 0) return this

    try {
      writeLock.lock

      var frTime = Long.MaxValue
      var toTime = Long.MinValue

      val lenth = values.length
      val shouldReverse = !isAscending(values)
      var i = if (shouldReverse) lenth - 1 else 0
      while (i >= 0 && i < lenth) {
        val value = values(i)
        if (value != null) {
          val time = value.time
          createOrReset(time)
          assignValue(value)

          frTime = math.min(frTime, time)
          toTime = math.max(toTime, time)
        } else {
          // @todo why will  happen? seems form loadFromPersistence
          log.warning("Value of i={} is null", i)
        }

        // shoudReverse: the recent quote's index is more in quotes, thus the order in timePositions[] is opposed to quotes
        // otherwise:    the recent quote's index is less in quotes, thus the order in timePositions[] is the same as quotes
        if (shouldReverse)
          i -= 1
        else
          i += 1
      }

      publish(TSerEvent.Updated(this, shortName, frTime, toTime))

    } finally {
      writeLock.unlock
    }

    log.debug("TimestampsLog: {}", timestamps.log)
    this
  }

  def isOnCalendarMode = _isOnCalendarMode
  def toOnCalendarMode { _isOnCalendarMode = true }
  def toOnOccurredMode { _isOnCalendarMode = false }

  def indexOfTime(time: Long): Int = activeTimestamps.indexOfOccurredTime(time)
  def timeOfIndex(idx: Int): Long = activeTimestamps(idx)

  def rowOfTime(time: Long): Int = activeTimestamps.rowOfTime(time, freq)
  def timeOfRow(row: Int): Long = activeTimestamps.timeOfRow(row, freq)
  def lastOccurredRow: Int = activeTimestamps.lastRow(freq)

  override def size: Int = activeTimestamps.sizeOf(freq)

  private def activeTimestamps: TStamps = {
    try {
      readLock.lock

      if (_isOnCalendarMode) timestamps.asOnCalendar else timestamps
    } finally {
      readLock.unlock
    }
  }
}
