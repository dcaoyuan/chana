package chana.timeseries

import chana.collection.AbstractArrayList
import java.util.Calendar
import java.util.ConcurrentModificationException
import java.util.GregorianCalendar
import java.util.TimeZone
import scala.reflect.ClassTag

/**
 *
 * The row always begin from 0, and corresponds to first occurred time
 *
 * onOccurred: ...........|xxxxxxxxxxxxxxx|............
 * index                   01234567....
 * row                     01234567....
 *
 * onCalendar: ...........|xxxxxooxxxxoooxxxxxoox|........
 * index                   01234  5678...
 * row                     01234567....
 *
 * @author  Caoyuan Deng
 * @version 1.02, 11/25/2006
 * @since   1.0.4
 */
final class TStampsLog(_initialSize: Int, _maxCapacity: Int) extends AbstractArrayList[Short](None) {
  import TStampsLog._

  def this() = this(16, Int.MaxValue)
  def this(initialSize: Int) = this(initialSize, Int.MaxValue)

  override protected def initialSize = if (_maxCapacity < _initialSize) _maxCapacity else _initialSize
  override protected def maxCapacity = _maxCapacity

  private var _logCursor = -1
  private var _logTime = System.currentTimeMillis

  def logCursor = _logCursor
  def logTime = _logTime

  def checkKind(logFlag: Short): Int = {
    logFlag & KIND
  }

  def checkSize(logFlag: Short): Int = {
    logFlag & SIZE
  }

  def logAppend(size: Int) {
    def addLog(size: Int) {
      if (size > SIZE) {
        this += (APPEND | SIZE).toShort
        _logCursor = nextCursor(_logCursor)
        addLog(size - SIZE)
      } else {
        this += (APPEND | size).toShort
        _logCursor = nextCursor(_logCursor)
      }
    }

    if (_logCursor >= 0) {
      val prev = apply(_logCursor)
      val prevKind = checkKind(prev)
      val prevSize = checkSize(prev)
      //println("Append log: prevKind=" + prevKind + ", prevCursor=" + _logCursor + ", prevSize=" + prevSize)
      if (prevKind == APPEND) {
        val newSize = prevSize + size
        if (newSize <= SIZE) {
          // merge with previous one
          //println("Append log (merged with prev): newSize=" + newSize)
          update(_logCursor, (APPEND | newSize).toShort)
        } else addLog(size)
      } else addLog(size)
    } else addLog(size)

    _logTime = System.currentTimeMillis
  }

  def logInsert(size: Int, idx: Int) {
    def addLog(size: Int, idx: Int) {
      if (size > SIZE) {
        this += (INSERT | SIZE).toShort
        this ++= intToShorts(idx)
        _logCursor = nextCursor(_logCursor)
        addLog(size - SIZE, idx + SIZE)
      } else {
        this += (INSERT | size).toShort
        this ++= intToShorts(idx)
        _logCursor = nextCursor(_logCursor)
      }
    }

    if (_logCursor >= 0) {
      val prev = apply(_logCursor)
      val prevKind = checkKind(prev)
      val prevSize = checkSize(prev)
      //println("Insert log: prevKind=" + prevKind + ", prevCursor=" + _logCursor + ", prevSize=" + prevSize + ", idx=" + idx)
      if (prevKind == INSERT) {
        val prevIdx = shortsToInt(apply(_logCursor + 1), apply(_logCursor + 2))
        if (prevIdx + prevSize == idx) {
          val newSize = prevSize + size
          if (newSize <= SIZE) {
            // merge with previous one
            //println("Insert log (merged with prev): idx=" + prevIdx + ", newSize=" + newSize)
            update(_logCursor, (INSERT | newSize).toShort)
          } else addLog(size, idx)
        } else addLog(size, idx)
      } else addLog(size, idx)
    } else addLog(size, idx)

    _logTime = System.currentTimeMillis
  }

  def insertIndexOfLog(cursor: Int): Int = {
    shortsToInt(apply(cursor + 1), apply(cursor + 2))
  }

  /** cursorIncr: if (prev == append) 1 else 3 */
  def nextCursor(cursor: Int): Int = {
    if (cursor == -1) {
      0
    } else {
      checkKind(apply(cursor)) match {
        case APPEND => cursor + 1
        case INSERT => cursor + 3
      }
    }
  }

  /* [0] = lowest order 16 bits; [1] = highest order 16 bits. */
  private def intToShorts(i: Int): Array[Short] = {
    Array((i >> 16).toShort, i.toShort)
  }

  private def shortsToInt(hi: Short, lo: Short) = {
    (hi << 16) + lo
  }

  override def toString: String = {
    val sb = new StringBuilder
    sb.append("TimestampsLog: cursor=").append(_logCursor).append(", size=").append(size).append(", content=")
    var i = 0
    while (i < size) {
      val flag = apply(i)
      checkKind(flag) match {
        case APPEND =>
          sb.append("A").append(checkSize(flag)).append(",")
          i += 1
        case INSERT =>
          sb.append("I").append(checkSize(flag)).append("@").append(shortsToInt(apply(i + 1), apply(i + 2))).append(",")
          i += 3
        case x => sb.append("\nflag").append(x).append("X").append(i).append(",")
      }
    }
    sb.toString
  }

  // --- methods inherited from traits

  def result: TStampsLog = this

  override def reverse: TStampsLog = {
    val reversed = new TStampsLog(size)
    var i = 0
    while (i < size) {
      reversed(i) = apply(size - 1 - i)
      i += 1
    }
    reversed
  }

  override def partition(p: Short => Boolean): (TStampsLog, TStampsLog) = {
    val l, r = new TStampsLog()
    for (x <- this) (if (p(x)) l else r) += x
    (l, r)
  }

  override def clone(): TStampsLog = new TStampsLog(size) ++= this
}

object TStampsLog {
  private val KIND = 0xC000 // 1100 0000 0000 0000
  private val SIZE = 0x3FFF // 0011 1111 1111 1111

  val APPEND = 0x0000 // 0000 0000 0000 0000
  val INSERT = 0x4000 // 0100 0000 0000 0000
  val REMOVE = 0x8000 // 1000 0000 0000 0000
  val NUMBER = 0xC000 // 1100 0000 0000 0000
}

abstract class TStamps(_initialSize: Int, _maxCapacity: Int) extends AbstractArrayList[Long](None) with Cloneable {
  val LONG_LONG_AGO = new GregorianCalendar(1900, Calendar.JANUARY, 1).getTimeInMillis

  private val readWriteLock = new java.util.concurrent.locks.ReentrantReadWriteLock
  val readLock = readWriteLock.readLock
  val writeLock = readWriteLock.writeLock

  val log = new TStampsLog(initialSize)

  override protected def initialSize = if (_maxCapacity < _initialSize) _maxCapacity else _initialSize
  override protected def maxCapacity = _maxCapacity

  def isOnCalendar: Boolean

  def asOnCalendar: TStamps

  /**
   * Get nearest row that can also properly extends before firstOccurredTime
   * or after lastOccurredTime
   */
  def rowOfTime(time: Long, freq: TFreq): Int

  def timeOfRow(row: Int, freq: TFreq): Long

  def lastRow(freq: TFreq): Int

  def sizeOf(freq: TFreq): Int

  def indexOfOccurredTime(time: Long): Int

  /**
   * Search the nearest index between '1' to 'lastIndex - 1'
   * We only need to use this computing in case of onOccurred.
   */
  def nearestIndexOfOccurredTime(time: Long): Int

  /**
   * @param time the time, inclusive
   * @return index of nearest behind time (include this time (if exist)),
   */
  def indexOrNextIndexOfOccurredTime(time: Long): Int

  /**
   * @param time the time, inclusive
   * @return index of nearest before or equal(if exist) time
   */
  def indexOrPrevIndexOfOccurredTime(time: Long): Int

  def firstOccurredTime: Long

  def lastOccurredTime: Long

  def iterator(freq: TFreq): TStampsIterator

  def iterator(freq: TFreq, fromTime: Long, toTime: Long, timeZone: TimeZone): TStampsIterator

  /**
   * This should not be an abstract method so that scalac knows it's a override of
   * @cloneable instead of java.lang.Object#clone
   */
  override def clone: TStamps = super.clone.asInstanceOf[TStamps]
}

/**
 *
 *
 * @author  Caoyuan Deng
 * @version 1.02, 11/25/2006
 * @since   1.0.4
 */
object TStamps {

  def apply(initialSize: Int, maxCapacity: Int): TStamps = new TStampsOnOccurred(initialSize, maxCapacity)

  private class TStampsOnOccurred(_initialSize: Int, _maxCapacity: Int) extends TStamps(_initialSize, _maxCapacity) {

    private val onCalendarShadow = new TStampsOnCalendar(this)

    def isOnCalendar: Boolean = false

    def asOnCalendar: TStamps = onCalendarShadow

    /**
     * Get nearest row that can also properly extends before firstOccurredTime
     * or after lastOccurredTime
     */
    def rowOfTime(time: Long, freq: TFreq): Int = {
      val lastOccurredIdx = size - 1
      if (lastOccurredIdx == -1) {
        return -1
      }

      val firstOccurredTime = apply(0)
      val lastOccurredTime = apply(lastOccurredIdx)
      if (time <= firstOccurredTime) {
        freq.nFreqsBetween(firstOccurredTime, time)
      } else if (time >= lastOccurredTime) {
        /**
         * @NOTICE
         * The number of bars of onOccurred between first-last is different
         * than onCalendar, so we should count from lastOccurredIdx in case
         * of onOccurred. so, NEVER try:
         * <code>return freq.nFreqsBetween(firstOccurredTime, time);</code>
         * in case of onOccurred
         */
        lastOccurredIdx + freq.nFreqsBetween(lastOccurredTime, time)
      } else {
        nearestIndexOfOccurredTime(time)
      }
    }

    /**
     * This is an efficent method
     */
    def timeOfRow(row: Int, freq: TFreq): Long = {
      val lastOccurredIdx = size - 1
      if (lastOccurredIdx < 0) {
        return 0
      }

      val firstOccurredTime = apply(0)
      val lastOccurredTime = apply(lastOccurredIdx)
      if (row < 0) {
        freq.timeAfterNFreqs(firstOccurredTime, row)
      } else if (row > lastOccurredIdx) {
        freq.timeAfterNFreqs(lastOccurredTime, row - lastOccurredIdx)
      } else {
        apply(row)
      }
    }

    def lastRow(freq: TFreq): Int = {
      val lastOccurredIdx = size - 1
      lastOccurredIdx
    }

    def sizeOf(freq: TFreq): Int = size

    def indexOfOccurredTime(time: Long): Int = {
      val size1 = size
      if (size1 == 0) {
        return -1
      } else if (size1 == 1) {
        if (apply(0) == time) {
          return 0
        } else {
          return -1
        }
      }

      var from = 0
      var to = size1 - 1
      var length = to - from
      while (length > 1) {
        length /= 2
        val midTime = apply(from + length)
        if (time > midTime) {
          from += length
        } else if (time < midTime) {
          to -= length
        } else {
          /** time == midTime */
          return from + length
        }
        length = to - from
      }

      /**
       * if we reach here, that means the time should between (start) and (start + 1),
       * and the length should be 1 (end - start). So, just do following checking,
       * if can't get exact index, just return -1.
       */
      if (time == apply(from)) {
        from
      } else if (time == apply(from + 1)) {
        from + 1
      } else {
        -1
      }
    }

    /**
     * Search the nearest index between '1' to 'lastIndex - 1'
     * We only need to use this computing in case of onOccurred.
     */
    def nearestIndexOfOccurredTime(time: Long): Int = {
      var from = 0
      var to = size - 1
      var length = to - from
      while (length > 1) {
        length /= 2
        val midTime = apply(from + length)
        if (time > midTime) {
          from += length
        } else if (time < midTime) {
          to -= length
        } else {
          /** time == midTime */
          return from + length
        }
        length = to - from
      }

      /**
       * if we reach here, that means the time should between (start) and (start + 1),
       * and the length should be 1 (end - start). So, just do following checking,
       * if can't get exact index, just return nearest one: 'start'
       */
      if (time == apply(from)) {
        from
      } else if (time == apply(from + 1)) {
        from + 1
      } else {
        from
      }
    }

    /**
     * return index of nearest behind time (include this time (if exist)),
     * @param time the time, inclusive
     */
    def indexOrNextIndexOfOccurredTime(time: Long): Int = {
      val size1 = size
      if (size1 == 0) {
        return -1
      } else if (size1 == 1) {
        if (apply(0) >= time) {
          return 0
        } else {
          return -1
        }
      }

      var from = 0
      var to = size1 - 1
      var length = to - from
      while (length > 1) {
        length /= 2
        val midTime = apply(from + length)
        if (time > midTime) {
          from += length
        } else if (time < midTime) {
          to -= length
        } else {
          /** time == midTime */
          return from + length
        }
        length = to - from
      }

      /**
       * if we reach here, that means the time should between (from) and (from + 1),
       * and the 'length' should be 1 (end - start). So, just do following checking.
       * If can't get exact index, ﻿just return invalid value -1
       */
      if (apply(from) >= time) {
        from
      } else if (apply(from + 1) >= time) {
        from + 1
      } else {
        -1
      }
    }

    /** return index of nearest before or equal(if exist) time */
    def indexOrPrevIndexOfOccurredTime(time: Long): Int = {
      val size1 = size
      if (size1 == 0) {
        return -1
      } else if (size1 == 1) {
        if (apply(0) <= time) {
          return 0
        } else {
          return -1
        }
      }

      var from = 0
      var to = size1 - 1
      var length = to - from
      while (length > 1) {
        length /= 2
        val midTime = apply(from + length)
        if (time > midTime) {
          from += length
        } else if (time < midTime) {
          to -= length
        } else {
          /** time == midTime */
          return from + length
        }
        length = to - from
      }

      /**
       * if we reach here, that means the time should between (from) and (from + 1),
       * and the 'length' should be 1 (end - start). So, just do following checking.
       * If can't get exact index, just return invalid -1.
       */
      if (apply(from + 1) <= time) {
        from + 1
      } else if (apply(from) <= time) {
        from
      } else {
        -1
      }
    }

    def firstOccurredTime: Long = {
      val size1 = size
      if (size1 > 0) apply(0) else 0
    }

    def lastOccurredTime: Long = {
      val size1 = size
      if (size1 > 0) apply(size1 - 1) else 0
    }

    def iterator(freq: TFreq): TStampsIterator = {
      new ItrOnOccurred(freq)
    }

    def iterator(freq: TFreq, fromTime: Long, toTime: Long, timeZone: TimeZone): TStampsIterator = {
      new ItrOnOccurred(freq, fromTime, toTime, timeZone)
    }

    override def clone: TStamps = {
      new TStampsOnOccurred(this.initialSize, this.maxCapacity) ++= this
    }

    class ItrOnOccurred(freq: TFreq, _fromTime: Long, toTime: Long, timeZone: TimeZone) extends TStampsIterator {
      private val cal = Calendar.getInstance(timeZone)

      val fromTime = freq.round(_fromTime, cal)

      def this(freq: TFreq) {
        this(freq, firstOccurredTime, lastOccurredTime, TimeZone.getDefault)
      }

      var cursorTime = fromTime
      /** Reset to LONG_LONG_AGO if this element is deleted by a call to remove. */
      var lastReturnTime = LONG_LONG_AGO

      /**
       * Row of element to be returned by subsequent call to next.
       */
      var cursorRow = 0

      /**
       * Index of element returned by most recent call to next or
       * previous.  Reset to -1 if this element is deleted by a call
       * to remove.
       */
      var lastRet = -1

      /**
       * The modCount value that the iterator believes that the backing
       * List should have.  If this expectation is violated, the iterator
       * has detected concurrent modification.
       */
      @transient @volatile
      var modCount: Long = _

      var expectedModCount = modCount

      def hasNext: Boolean = {
        cursorTime <= toTime
      }

      def next: Long = {
        checkForComodification
        try {
          cursorRow += 1
          val next = if (cursorRow >= size) freq.nextTime(cursorTime) else apply(cursorRow)
          cursorTime = next
          lastReturnTime = cursorTime
          return next
        } catch {
          case e: IndexOutOfBoundsException =>
            checkForComodification
            throw new NoSuchElementException
        }
      }

      def checkForComodification: Unit = {
        if (modCount != expectedModCount)
          throw new ConcurrentModificationException
      }

      def hasPrev: Boolean = {
        cursorTime >= fromTime
      }

      def prev: Long = {
        checkForComodification
        try {
          cursorRow -= 1
          val prev1 = if (cursorRow < 0) freq.prevTime(cursorTime) else apply(cursorRow)
          cursorTime = prev1
          lastReturnTime = cursorTime
          return prev1
        } catch {
          case e: IndexOutOfBoundsException =>
            checkForComodification
            throw new NoSuchElementException
        }
      }

      def nextOccurredIndex: Int = {
        indexOrNextIndexOfOccurredTime(cursorTime)
      }

      def prevOccurredIndex: Int = {
        indexOrPrevIndexOfOccurredTime(cursorTime)
      }

      def nextRow: Int = {
        cursorRow
      }

      def prevRow: Int = {
        cursorRow - 1
      }
    }

    // --- methods inherited from traits

    def result: TStampsOnOccurred = this

    override def reverse: TStampsOnOccurred = {
      val reversed = new TStampsOnOccurred(this.initialSize, this.maxCapacity)
      var i = 0
      while (i < size) {
        reversed += apply(size - 1 - i)
        i += 1
      }
      reversed
    }

    /**
     * @todo
     */
    @deprecated("Unsupported operation", "0")
    override def partition(p: Long => Boolean): (TStampsOnOccurred, TStampsOnOccurred) = {
      throw new UnsupportedOperationException()
    }
  }

  /**
   * A shadow and extrem lightweight class for Timestamps, it will be almost the same
   * instance as delegateTimestamps, especially shares the elements data. Except its
   * isOnCalendar() always return true.
   * Why not to use Proxy.class ? for performance reason.
   */
  private class TStampsOnCalendar(delegateTimestamps: TStamps) extends TStamps(0, Int.MaxValue) {
    /**
     * the timestamps to be wrapped, it not necessary to be a TimestampsOnOccurred,
     * any class implemented Timestamps is ok.
     */
    def isOnCalendar: Boolean = true

    def asOnCalendar: TStamps = delegateTimestamps.asOnCalendar

    /**
     * Get nearest row that can also properly extends before firstOccurredTime
     * or after lastOccurredTime
     */
    def rowOfTime(time: Long, freq: TFreq): Int = {
      val lastOccurredIdx = size - 1
      if (lastOccurredIdx == -1) {
        -1
      } else {
        val firstOccurredTime = apply(0)
        freq.nFreqsBetween(firstOccurredTime, time)
      }
    }

    /**
     * This is an efficent method
     */
    def timeOfRow(row: Int, freq: TFreq): Long = {
      val lastOccurredIdx = size - 1
      if (lastOccurredIdx < 0) {
        0
      } else {
        val firstOccurredTime = apply(0)
        freq.timeAfterNFreqs(firstOccurredTime, row)
      }
    }

    def lastRow(freq: TFreq): Int = {
      val lastOccurredIdx = size - 1
      if (lastOccurredIdx < 0) {
        0
      } else {
        val firstOccurredTime = apply(0)
        val lastOccurredTime = apply(lastOccurredIdx)
        freq.nFreqsBetween(firstOccurredTime, lastOccurredTime)
      }
    }

    def sizeOf(freq: TFreq): Int = {
      lastRow(freq) + 1
    }

    /** -------------------------------------------- */

    def indexOfOccurredTime(time: Long) = delegateTimestamps.indexOfOccurredTime(time)

    def nearestIndexOfOccurredTime(time: Long) = delegateTimestamps.nearestIndexOfOccurredTime(time)

    def indexOrNextIndexOfOccurredTime(time: Long) = delegateTimestamps.indexOrNextIndexOfOccurredTime(time)

    /** return index of nearest before or equal (if exist) time */
    def indexOrPrevIndexOfOccurredTime(time: Long) = delegateTimestamps.indexOrPrevIndexOfOccurredTime(time)

    def firstOccurredTime = delegateTimestamps.firstOccurredTime

    def lastOccurredTime = delegateTimestamps.lastOccurredTime

    override def size = delegateTimestamps.size

    override def isEmpty = delegateTimestamps.isEmpty

    override def iterator = delegateTimestamps.iterator

    override def toArray[B >: Long](implicit m: ClassTag[B]): Array[B] = delegateTimestamps.toArray(m)

    override def toArray: Array[Long] = delegateTimestamps.toArray

    override def copyToArray[B >: Long](xs: Array[B], start: Int) = delegateTimestamps.copyToArray(xs, start)

    override def sliceToArray(start: Int, len: Int): Array[Long] = delegateTimestamps.sliceToArray(start, len)

    override def +(elem: Long) = { delegateTimestamps + elem; this }

    override def +=(elem: Long) = this.+(elem)

    override def ++(xs: TraversableOnce[Long]) = { delegateTimestamps ++ xs; this }

    override def ++=(xs: TraversableOnce[Long]) = this.++(xs)

    override def +:(elem: Long) = { delegateTimestamps.+:(elem); this }

    override def +=:(elem: Long) = this.+:(elem)

    override def ++:(xs: TraversableOnce[Long]) = { delegateTimestamps.insertAll(0, xs.toTraversable); this }

    override def ++=:(xs: TraversableOnce[Long]) = this.++:(xs)

    override def insertOne(n: Int, elem: Long) = delegateTimestamps.insertOne(n, elem)

    override def insertAll(n: Int, elems: scala.collection.Traversable[Long]) = delegateTimestamps.insertAll(n, elems)

    override def remove(idx: Int) = delegateTimestamps.remove(idx)

    override def contains[A1 >: Long](elem: A1) = delegateTimestamps.contains(elem)

    override def clear = delegateTimestamps.clear

    override def equals(o: Any) = delegateTimestamps.equals(o)

    override def hashCode = delegateTimestamps.hashCode

    override def apply(index: Int) = delegateTimestamps.apply(index)

    override def update(index: Int, element: Long) = delegateTimestamps.update(index, element)

    override def indexOf[B >: Long](o: B) = delegateTimestamps.indexOf(o)

    override def lastIndexOf[B >: Long](o: B) = delegateTimestamps.lastIndexOf(o)

    def iterator(freq: TFreq) = new ItrOnCalendar(freq)

    def iterator(freq: TFreq, fromTime: Long, toTime: Long, timeZone: TimeZone) = new ItrOnCalendar(freq, fromTime, toTime, timeZone)

    @transient @volatile
    protected var modCount: Long = 0

    override def clone: TStampsOnCalendar = new TStampsOnCalendar(delegateTimestamps.clone)

    class ItrOnCalendar(freq: TFreq, _fromTime: Long, toTime: Long, timeZone: TimeZone) extends TStampsIterator {
      private val cal = Calendar.getInstance(timeZone)

      val fromTime = freq.round(_fromTime, cal)

      def this(freq: TFreq) {
        this(freq, firstOccurredTime, lastOccurredTime, TimeZone.getDefault)
      }

      var cursorTime = fromTime
      /** Reset to LONG_LONG_AGO if this element is deleted by a call to remove. */
      var lastReturnTime = LONG_LONG_AGO

      /**
       * Row of element to be returned by subsequent call to next.
       */
      var cursorRow = 0

      /**
       * Index of element returned by most recent call to next or
       * previous.  Reset to -1 if this element is deleted by a call
       * to remove.
       */
      var lastRet = -1

      /**
       * The modCount value that the iterator believes that the backing
       * List should have.  If this expectation is violated, the iterator
       * has detected concurrent modification.
       */
      var expectedModCount = modCount

      def hasNext: Boolean = {
        cursorTime <= toTime
      }

      def next: Long = {
        checkForComodification
        try {
          cursorRow += 1
          val next = freq.nextTime(cursorTime)
          cursorTime = next
          lastReturnTime = cursorTime
          return next
        } catch {
          case e: IndexOutOfBoundsException =>
            checkForComodification
            throw new NoSuchElementException
        }
      }

      def checkForComodification: Unit = {
        if (modCount != expectedModCount) {
          throw new ConcurrentModificationException
        }
      }

      def hasPrev: Boolean = {
        cursorTime >= fromTime
      }

      def prev: Long = {
        checkForComodification
        try {
          cursorRow -= 1
          val prev1 = freq.prevTime(cursorTime)
          cursorTime = prev1
          lastReturnTime = cursorTime
          return prev1
        } catch {
          case e: IndexOutOfBoundsException =>
            checkForComodification
            throw new NoSuchElementException
        }
      }

      def nextOccurredIndex: Int = {
        indexOrNextIndexOfOccurredTime(cursorTime)
      }

      def prevOccurredIndex: Int = {
        indexOrPrevIndexOfOccurredTime(cursorTime)
      }

      def nextRow: Int = cursorRow

      def prevRow: Int = { cursorRow - 1 }
    }

    // --- methods inherited from traits

    def result: TStampsOnCalendar = this

    override def reverse: TStampsOnCalendar = {
      new TStampsOnCalendar(delegateTimestamps.reverse.asInstanceOf[TStamps])
    }

    /**
     * @todo
     */
    @deprecated("Unsupported operation", "0")
    override def partition(p: Long => Boolean): (TStampsOnCalendar, TStampsOnCalendar) = {
      throw new UnsupportedOperationException()
    }
  }
}

/**
 *
 * @author  Caoyuan Deng
 * @version 1.0, 11/24/2006
 * @since   1.0.4
 */
trait TStampsIterator {

  def hasNext: Boolean

  def next: Long

  def hasPrev: Boolean

  def prev: Long

  def nextOccurredIndex: Int

  def prevOccurredIndex: Int

  def nextRow: Int

  def prevRow: Int
}
