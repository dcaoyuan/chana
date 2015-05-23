package chana.timeseries

import chana.timeseries.TUnit._
import java.util.Calendar
import java.util.regex.Pattern

/**
 * Class combining Unit and nUnits.
 * Try to implement a Primitive-like type.
 * Use modifies to define a lightweight class.
 *
 * This class is better to be treated as a <b>value</b> class, so, using:
 *   <code>freq = anotherFreq.clone()</code>
 * instead of:
 *   <code>freq = anotherFreq</code>
 * is always more safe.
 *
 * @author Caoyuan Deng
 */
class TFreq(val unit: TUnit, val nUnits: Int) extends Cloneable with Ordered[TFreq] with Serializable {

  def this() = this(TUnit.Day, 1)

  /**
   * interval in milliseconds
   */
  val interval = unit.interval * nUnits

  def nextTime(fromTime: Long): Long = unit.timeAfterNUnits(fromTime, nUnits)

  def prevTime(fromTime: Long): Long = unit.timeAfterNUnits(fromTime, -nUnits)

  def timeAfterNFreqs(fromTime: Long, nFreqs: Int): Long = unit.timeAfterNUnits(fromTime, nUnits * nFreqs)

  def nFreqsBetween(fromTime: Long, toTime: Long): Int = unit.nUnitsBetween(fromTime, toTime) / nUnits

  /**
   * round time to freq's begin 0
   * @param time time in milliseconds from the epoch (1 January 1970 0:00 UTC)
   * @param cal Calendar instance with proper timeZone set, <b>cal is not thread safe</b>
   */
  def round(time: Long, cal: Calendar): Long = {
    cal.setTimeInMillis(time)
    val offsetToLocalZeroOfDay = cal.getTimeZone.getRawOffset + cal.get(Calendar.DST_OFFSET)
    ((time + offsetToLocalZeroOfDay) / interval) * interval - offsetToLocalZeroOfDay
  }

  /**
   * @param timeA time in milliseconds from the epoch (1 January 1970 0:00 UTC)
   * @param timeB time in milliseconds from the epoch (1 January 1970 0:00 UTC)
   * @param cal Calendar instance with proper timeZone set, <b>cal is not thread safe</b>
   *
   * @todo use nUnits
   */
  def sameInterval(timeA: Long, timeB: Long, cal: Calendar): Boolean = {
    unit match {
      case TUnit.Week =>
        cal.setTimeInMillis(timeA)
        val weekOfYearA = cal.get(Calendar.WEEK_OF_YEAR)
        cal.setTimeInMillis(timeB)
        val weekOfYearB = cal.get(Calendar.WEEK_OF_YEAR)
        weekOfYearA == weekOfYearB
      case TUnit.Month =>
        cal.setTimeInMillis(timeA)
        val monthOfYearA = cal.get(Calendar.MONTH)
        val yearA = cal.get(Calendar.YEAR)
        cal.setTimeInMillis(timeB)
        val monthOfYearB = cal.get(Calendar.MONTH)
        val yearB = cal.get(Calendar.YEAR)
        yearA == yearB && monthOfYearA == monthOfYearB
      case TUnit.Year =>
        cal.setTimeInMillis(timeA)
        val yearA = cal.get(Calendar.YEAR)
        cal.setTimeInMillis(timeB)
        val yearB = cal.get(Calendar.YEAR)
        yearA == yearB
      case _ =>
        round(timeA, cal) == round(timeB, cal)
    }
  }

  val name: String = {
    if (nUnits == 1) {
      unit match {
        case Hour | Day | Week | Month | Year => unit.longName
        case _                                => nUnits + unit.compactName
      }
    } else {
      nUnits + unit.compactName + "s"
    }
  }

  val shortName: String = nUnits + unit.shortName
  val compactName: String = nUnits + unit.compactName

  override def equals(o: Any): Boolean = {
    o match {
      case x: TFreq => this.interval == x.interval
      case _        => false
    }
  }

  override def hashCode: Int = {
    /** should let the equaled frequencies have the same hashCode, just like a Primitive type */
    (interval ^ (interval >>> 32)).toInt
  }

  override def clone: TFreq = {
    try {
      super.clone.asInstanceOf[TFreq]
    } catch {
      case ex: CloneNotSupportedException => ex.printStackTrace; null
    }
  }

  override def compare(another: TFreq): Int = {
    if (this.unit.interval < another.unit.interval) {
      -1
    } else if (this.unit.interval > another.unit.interval) {
      1
    } else {
      if (this.nUnits < another.nUnits) -1 else { if (this.nUnits == another.nUnits) 0 else 1 }
    }
  }

  override def toString: String = name
}

object TFreq {
  val PREDEFINED = Set(ONE_MIN,
    TWO_MINS,
    THREE_MINS,
    FOUR_MINS,
    FIVE_MINS,
    FIFTEEN_MINS,
    THIRTY_MINS,
    DAILY,
    TWO_DAYS,
    THREE_DAYS,
    FOUR_DAYS,
    FIVE_DAYS,
    WEEKLY,
    MONTHLY)

  case object SELF_DEFINED extends TFreq(TUnit.Second, 1)
  case object ONE_SEC extends TFreq(TUnit.Second, 1)
  case object TWO_SECS extends TFreq(TUnit.Second, 2)
  case object THREE_SECS extends TFreq(TUnit.Second, 3)
  case object FOUR_SECS extends TFreq(TUnit.Second, 3)
  case object FIVE_SECS extends TFreq(TUnit.Second, 5)
  case object FIFTEEN_SECS extends TFreq(TUnit.Second, 15)
  case object THIRTY_SECS extends TFreq(TUnit.Second, 30)
  case object ONE_MIN extends TFreq(TUnit.Minute, 1)
  case object TWO_MINS extends TFreq(TUnit.Minute, 2)
  case object THREE_MINS extends TFreq(TUnit.Minute, 3)
  case object FOUR_MINS extends TFreq(TUnit.Minute, 4)
  case object FIVE_MINS extends TFreq(TUnit.Minute, 5)
  case object FIFTEEN_MINS extends TFreq(TUnit.Minute, 15)
  case object THIRTY_MINS extends TFreq(TUnit.Minute, 30)
  case object ONE_HOUR extends TFreq(TUnit.Hour, 1)
  case object DAILY extends TFreq(TUnit.Day, 1)
  case object TWO_DAYS extends TFreq(TUnit.Day, 2)
  case object THREE_DAYS extends TFreq(TUnit.Day, 3)
  case object FOUR_DAYS extends TFreq(TUnit.Day, 4)
  case object FIVE_DAYS extends TFreq(TUnit.Day, 5)
  case object WEEKLY extends TFreq(TUnit.Week, 1)
  case object MONTHLY extends TFreq(TUnit.Month, 1)
  case object THREE_MONTHS extends TFreq(TUnit.Month, 3)
  case object ONE_YEAR extends TFreq(TUnit.Year, 1)

  private val shortNamePattern = Pattern.compile("([0-9]+)([smhDWMY])")

  def withName(shortName: String): Option[TFreq] = {
    val matcher = shortNamePattern.matcher(shortName)
    if (matcher.find && matcher.groupCount == 2) {
      val nUnits = matcher.group(1).toInt
      TUnit.withShortName(matcher.group(2)) match {
        case Some(unit) => Some(TFreq(unit, nUnits))
        case None       => None
      }
    } else None
  }

  def apply(unit: TUnit, nUnit: Int) = new TFreq(unit, nUnit)

  // simple test
  def main(args: Array[String]) {
    val tz = java.util.TimeZone.getTimeZone("America/New_York")

    val df = new java.text.SimpleDateFormat("MM/dd/yyyy h:mma")
    df.setTimeZone(tz)

    val date0 = df.parse("6/18/2010 0:00am")
    val time0 = date0.getTime // 1276833600000

    val date1 = df.parse("6/18/2010 4:00pm")
    val time1 = date1.getTime // 1276891200000

    val cal = Calendar.getInstance(tz)
    val rounded0 = TFreq.DAILY.round(time0, cal)
    if (rounded0 == time0)
      println(time0 + " was properly rounded")
    else
      println("Error: " + time0 + " should be rounded to " + time0 + ", but was wrongly rounded to " + rounded0 + " !!!")

    val rounded1 = TFreq.DAILY.round(time1, cal)
    if (rounded1 == time0)
      println(time1 + " was properly rounded")
    else
      println("Error: " + time1 + " should be rounded to " + time0 + ", but was wrongly rounded to " + rounded1 + " !!!")
  }
}
