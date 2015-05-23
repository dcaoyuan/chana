package chana.timeseries

import java.text.DateFormat
import java.text.FieldPosition
import java.util.Calendar
import java.util.Date
import java.util.TimeZone

/**
 *
 *
 *
 * @NOTICE: Should avoid declaring Calendar instance as static, it's not thread-safe
 * see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6231579
 *
 * As Unit is enum, which actually is a kind of singleton, so delcare a
 * none static Calendar instance here also means an almost static instance,
 * so, if we declare class scope instance of Calendar in enum, we should also
 * synchronized each method that uses this instance or declare the cal
 * instance as volatile to share this instance by threads.
 *
 * @author Caoyuan Deng
 *
 * @credits:
 *     stebridev@users.sourceforge.net - fix case of Week : beginTimeOfUnitThatInclude(long)
 */
abstract class TUnit(val interval: Long) extends Serializable {
  import TUnit._

  /**
   * round time to unit's begin 0
   * @param time time in milliseconds from the epoch (1 January 1970 0:00 UTC)
   */
  def round(cal: Calendar): Long = {
    this match {
      case Day =>
        roundToDay(cal)
      case Week =>
        /**
         * set the time to this week's first day of one week
         *     int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK)
         *     calendar.add(Calendar.DAY_OF_YEAR, -(dayOfWeek - Calendar.SUNDAY))
         *
         * From stebridev@users.sourceforge.net:
         * In some place of the world the first day of month is Monday,
         * not Sunday like in the United States. For example Sunday 15
         * of August of 2004 is the week 33 in Italy and not week 34
         * like in US, while Thursday 19 of August is in the week 34 in
         * boot Italy and US.
         */
        val firstDayOfWeek = cal.getFirstDayOfWeek
        cal.set(Calendar.DAY_OF_WEEK, firstDayOfWeek)
        roundToDay(cal)
      case Month =>
        /** set the time to this month's 1st day */
        val dayOfMonth = cal.get(Calendar.DAY_OF_MONTH)
        cal.add(Calendar.DAY_OF_YEAR, -(dayOfMonth - 1))
        roundToDay(cal)
      case Year =>
        cal.set(Calendar.DAY_OF_YEAR, 1)
        roundToDay(cal)
      case _ =>
        val time = round(cal.getTimeInMillis)
        cal.setTimeInMillis(time)
    }

    cal.getTimeInMillis
  }

  def round(time: Long): Long = {
    //return (time + offsetToUTC / getInterval()) * getInterval() - offsetToUTC
    (time / interval) * interval
  }

  private def roundToDay(cal: Calendar) {
    val time = Day.round(cal.getTimeInMillis)
    cal.setTimeInMillis(time)
  }

  def name: String

  def shortName: String

  def compactName: String

  def longName: String

  def nUnitsBetween(fromTime: Long, toTime: Long): Int = {
    this match {
      case Week  => nWeeksBetween(fromTime, toTime)
      case Month => nMonthsBetween(fromTime, toTime)
      case _     => ((toTime - fromTime) / interval).toInt
    }
  }

  private def nWeeksBetween(fromTime: Long, toTime: Long): Int = {
    val between = ((toTime - fromTime) / ONE_WEEK).toInt

    /**
     * If between >= 1, between should be correct.
     * Otherwise, the days between fromTime and toTime is <= 6,
     * we should consider it as following:
     */
    if (math.abs(between) < 1) {
      val cal = Calendar.getInstance
      cal.setTimeInMillis(fromTime)
      val weekOfYearA = cal.get(Calendar.WEEK_OF_YEAR)

      cal.setTimeInMillis(toTime)
      val weekOfYearB = cal.get(Calendar.WEEK_OF_YEAR)

      /** if is in same week, between = 0, else between = 1 */
      if (weekOfYearA == weekOfYearB) 0 else { if (between > 0) 1 else -1 }
    } else {
      between
    }
  }

  private def nMonthsBetween(fromTime: Long, toTime: Long): Int = {
    val cal = Calendar.getInstance
    cal.setTimeInMillis(fromTime)
    val monthOfYearA = cal.get(Calendar.MONTH)
    val yearA = cal.get(Calendar.YEAR)

    cal.setTimeInMillis(toTime)
    val monthOfYearB = cal.get(Calendar.MONTH)
    val yearB = cal.get(Calendar.YEAR)

    /** here we assume each year has 12 months */
    (yearB * 12 + monthOfYearB) - (yearA * 12 + monthOfYearA)
  }

  def timeAfterNUnits(fromTime: Long, nUnits: Int): Long = {
    this match {
      case Week  => timeAfterNWeeks(fromTime, nUnits)
      case Month => timeAfterNMonths(fromTime, nUnits)
      case _     => fromTime + nUnits * interval
    }
  }

  /** snapped to first day of the week */
  private def timeAfterNWeeks(fromTime: Long, nWeeks: Int): Long = {
    val cal = Calendar.getInstance
    cal.setTimeInMillis(fromTime)

    /** set the time to first day of this week */
    val firstDayOfWeek = cal.getFirstDayOfWeek
    cal.set(Calendar.DAY_OF_WEEK, firstDayOfWeek)

    cal.add(Calendar.WEEK_OF_YEAR, nWeeks)

    cal.getTimeInMillis
  }

  /** snapped to 1st day of the month */
  private def timeAfterNMonths(fromTime: Long, nMonths: Int): Long = {
    val cal = Calendar.getInstance
    cal.setTimeInMillis(fromTime)

    /** set the time to this month's 1st day */
    val dayOfMonth = cal.get(Calendar.DAY_OF_MONTH)
    cal.add(Calendar.DAY_OF_YEAR, -(dayOfMonth - 1))

    cal.add(Calendar.MONTH, nMonths)

    cal.getTimeInMillis
  }

  def beginTimeOfUnitThatInclude(time: Long, cal: Calendar): Long = {
    cal.setTimeInMillis(time)
    round(cal)
  }

  def formatNormalDate(date: Date, timeZone: TimeZone): String = {
    val df = this match {
      case Second => DateFormat.getTimeInstance(DateFormat.MEDIUM)
      case Minute => DateFormat.getTimeInstance(DateFormat.SHORT)
      case Hour   => DateFormat.getTimeInstance(DateFormat.MEDIUM)
      case Day    => DateFormat.getDateInstance(DateFormat.SHORT)
      case Week   => DateFormat.getDateInstance(DateFormat.SHORT)
      case _      => DateFormat.getDateInstance(DateFormat.SHORT)
    }

    df.setTimeZone(timeZone)
    df.format(date)
  }

  def formatStrideDate(date: Date, timeZone: TimeZone): String = {
    val df = this match {
      case Second => DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT)
      case Minute => DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT)
      case Hour   => DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT)
      case Day    => DateFormat.getDateInstance(DateFormat.SHORT)
      case Week   => DateFormat.getDateInstance(DateFormat.SHORT)
      case _      => DateFormat.getDateInstance(DateFormat.SHORT)
    }

    val buffer = new StringBuffer
    df.setTimeZone(timeZone)
    df.format(date, buffer, new FieldPosition(DateFormat.MONTH_FIELD))
    buffer.toString()
  }

  override def equals(o: Any): Boolean = {
    o match {
      case x: TUnit => x.interval == this.interval
      case _        => false
    }
  }

  override def hashCode: Int = {
    /** should let the equaled frequencies have the same hashCode, just like a Primitive type */
    (interval ^ (interval >>> 32)).toInt
  }
}

object TUnit {
  /**
   * Interval of each Unit
   */
  private val ONE_SECOND: Int = 1000
  private val ONE_MINUTE: Int = 60 * ONE_SECOND
  private val ONE_HOUR: Int = 60 * ONE_MINUTE
  private val ONE_DAY: Long = 24 * ONE_HOUR
  private val ONE_WEEK: Long = 7 * ONE_DAY
  private val ONE_MONTH: Long = 30 * ONE_DAY
  private val ONE_YEAR: Long = (365.24 * ONE_DAY).toLong

  case object Second extends TUnit(ONE_SECOND) { val name = "Second"; val shortName = "s"; val compactName = "Sec"; val longName = "Second" }
  case object Minute extends TUnit(ONE_MINUTE) { val name = "Minute"; val shortName = "m"; val compactName = "Min"; val longName = "Minute" }
  case object Hour extends TUnit(ONE_HOUR) { val name = "Hour"; val shortName = "h"; val compactName = "Hour"; val longName = "Hourly" }
  case object Day extends TUnit(ONE_DAY) { val name = "Day"; val shortName = "D"; val compactName = "Day"; val longName = "Daily" }
  case object Week extends TUnit(ONE_WEEK) { val name = "Week"; val shortName = "W"; val compactName = "Week"; val longName = "Weekly" }
  case object Month extends TUnit(ONE_MONTH) { val name = "Month"; val shortName = "M"; val compactName = "Month"; val longName = "Monthly" }
  case object Year extends TUnit(ONE_YEAR) { val name = "Year"; val shortName = "Y"; val compactName = "Year"; val longName = "Yearly" }

  def values: Array[TUnit] = Array(
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Year)

  def withName(name: String) = {
    name match {
      case "Second" => Second
      case "Minute" => Minute
      case "Hour"   => Hour
      case "Day"    => Day
      case "Week"   => Week
      case "Month"  => Month
      case "Year"   => Year
      case c        => throw new Exception("Wrong unit: " + c)
    }
  }

  def withShortName(shortName: String): Option[TUnit] = {
    shortName match {
      case "s" => Some(Second)
      case "m" => Some(Minute)
      case "h" => Some(Hour)
      case "D" => Some(Day)
      case "W" => Some(Week)
      case "M" => Some(Month)
      case "Y" => Some(Year)
      case _   => None
    }
  }
}
