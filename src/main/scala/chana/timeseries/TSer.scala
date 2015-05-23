package chana.timeseries

import akka.actor.ActorRef
import chana.reactor.Reactor
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.mutable

/**
 * Time Series
 *
 *
 * @author Caoyuan Deng
 */
object TSer {
  final case class Export(commander: ActorRef, freq: TFreq, fromTime: Long, toTime: Long, limit: Int = Int.MaxValue)
}

/**
 * trait TBaseSer extends TSer, DefaultTBaseSer extends both TSer and TBaseSer, so
 * keep TSer as a trait instead of abstract class.
 */
trait TSer extends Reactor {

  private val readWriteLock = new ReentrantReadWriteLock
  protected val readLock = readWriteLock.readLock
  protected val writeLock = readWriteLock.writeLock

  reactions += {
    case TSer.Export(commander, _freq, fromTime, toTime, limit) if _freq == freq =>
      commander ! export(fromTime, toTime, limit)
  }

  private var _isLoaded: Boolean = false
  def isLoaded = _isLoaded
  def isLoaded_=(b: Boolean) {
    if (b) _isInLoading = false
    _isLoaded = b
  }

  private var _isInLoading: Boolean = false
  def isInLoading = _isInLoading
  def isInLoading_=(b: Boolean) {
    _isInLoading = b
  }

  /**
   * The vars that will be exported by export(...)
   */
  def exportableVars: Seq[TVar[_]] = vars

  /**
   * Export times and vars to map. Only Var with no-empty name can be exported.
   * The key of times is always "."
   *
   * @Note use collection.Map[String, Array[_]] here will cause some caller of
   * this method to be comipled with lots of stack space and time.
   * and use collection.Map[String, Array[Any]] wil cause runtime exception of
   * cast Array[T] (where T is primary type) to Array[Object]
   *
   * @Todo a custom vmap ?
   * @return usally a collection.Map[String, Array[_]]
   */
  def export(fromTime: Long, toTime: Long, limit: Int = Int.MaxValue): collection.Map[String, Any] = {
    try {
      readLock.lock
      timestamps.readLock.lock

      if (size > 0) {
        val frIdx = timestamps.indexOrNextIndexOfOccurredTime(fromTime)
        if (frIdx >= 0) {
          var toIdx = timestamps.indexOrPrevIndexOfOccurredTime(toTime)
          if (toIdx >= 0) {
            // best effort to avoid index out of bounds
            val vs = exportableVars filter (v => v.name != null && v.name != "")
            toIdx = vs.foldLeft(toIdx) { (acc, v) => math.min(acc, v.values.length) }
            if (toIdx >= frIdx) {
              var len = toIdx - frIdx + 1
              var retToTime = toTime
              if (len > limit) {
                len = limit
                retToTime = timestamps(frIdx + len - 1)
              }
              val vmap = new mutable.HashMap[String, Array[_]]()

              val times = timestamps.sliceToArray(frIdx, len)
              vmap.put(".", times)

              for (v <- vs) {
                val values = v.values.sliceToArray(frIdx, len)
                vmap.put(v.name, values)
              }

              vmap
            } else { /*println("toIdx < frIdx"); */ Map() }
          } else { /*println("toIdx < 0");  */ Map() }
        } else { /*println("frIdx < 0");  */ Map() }
      } else { /*println("size <= 0");  */ Map() }

    } finally {
      timestamps.readLock.unlock
      readLock.unlock
    }
  }

  protected def isAscending[V <: TVal](values: Array[V]): Boolean = {
    val size = values.length
    if (size <= 1) {
      true
    } else {
      var i = 0
      while (i < size - 1) {
        if (values(i).time < values(i + 1).time) {
          return true
        } else if (values(i).time > values(i + 1).time) {
          return false
        }
        i += 1
      }
      false
    }
  }

  def nonExists(time: Long) = !exists(time)

  // --- for charting

  /** horizonal grids of this indicator used to draw grid */
  var grids: Array[Double] = Array()

  private var _isOverlapping = false
  def isOverlapping = _isOverlapping
  def isOverlapping_=(b: Boolean) {
    _isOverlapping = b
  }

  override def toString: String = {
    getClass.getSimpleName + "(" + freq + ")"
  }

  // --- abstract methods
  def freq: TFreq

  def timestamps: TStamps
  def attach(timestamps: TStamps)

  def vars: Seq[TVar[_]]

  def exists(time: Long): Boolean

  def firstOccurredTime: Long
  def lastOccurredTime: Long

  def size: Int

  def indexOfOccurredTime(time: Long): Int

  /** clear(long fromTime) instead of clear(int fromIndex) to avoid bad usage */
  def clear(fromTime: Long)

  def shortName: String
  def shortName_=(shortName: String)
  def longName: String
  def displayName: String

  def validate
}

trait TSerEvent {
  def source: TSer
  def symbol: String
  def fromTime: Long
  def toTime: Long
  def message: String
  def callback: TSerEvent.Callback
}
object TSerEvent {
  type Callback = () => Any

  final case class Refresh(
    source: TSer,
    symbol: String,
    fromTime: Long,
    toTime: Long,
    message: String = null,
    callback: Callback = null) extends TSerEvent
  final case class Loaded(
    source: TSer,
    symbol: String,
    fromTime: Long,
    toTime: Long,
    message: String = null,
    callback: Callback = null) extends TSerEvent
  final case class Updated(
    source: TSer,
    symbol: String,
    fromTime: Long,
    toTime: Long,
    message: String = null,
    callback: Callback = null) extends TSerEvent
  final case class Closed(
    source: TSer,
    symbol: String,
    fromTime: Long,
    toTime: Long,
    message: String = null,
    callback: Callback = null) extends TSerEvent
  final case class Computed(
    source: TSer,
    symbol: String,
    fromTime: Long,
    toTime: Long,
    message: String = null,
    callback: Callback = null) extends TSerEvent
  final case class Cleared(
    source: TSer,
    symbol: String,
    fromTime: Long,
    toTime: Long,
    message: String = null,
    callback: Callback = null) extends TSerEvent
  final case class ToBeSet(
    source: TSer,
    symbol: String,
    fromTime: Long,
    toTime: Long,
    message: String = null,
    callback: Callback = null) extends TSerEvent

  def unapply(e: TSerEvent): Option[(TSer, String, Long, Long, String, Callback)] = {
    Some((e.source, e.symbol, e.fromTime, e.toTime, e.message, e.callback))
  }
}

