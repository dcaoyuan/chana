package chana

import java.lang.ref.SoftReference
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.TimeZone

/**
 * @author Caoyuan Deng
 */
package object util {

  private val defaultDfPattern = "yyyy-MM-dd HH:mm:ss"
  private val dfTl = new ThreadLocal[SoftReference[DateFormat]]()
  def dateFormatOf(tz: TimeZone = TimeZone.getDefault, pattern: String = defaultDfPattern): DateFormat = {
    val ref = dfTl.get
    if (ref != null) {
      val instance = ref.get
      if (instance != null) {
        instance.setTimeZone(tz)
        instance.asInstanceOf[SimpleDateFormat].applyPattern(pattern)
        return instance
      }
    }

    val instance = new SimpleDateFormat(pattern)
    instance.setTimeZone(tz)
    dfTl.set(new SoftReference[DateFormat](instance))
    instance
  }

  private val calTl = new ThreadLocal[SoftReference[Calendar]]()
  def calendarOf(tz: TimeZone = TimeZone.getDefault): Calendar = {
    val ref = calTl.get
    if (ref != null) {
      val instance = ref.get
      if (instance != null) {
        instance.setTimeZone(tz)
        return instance
      }
    }

    val instance = Calendar.getInstance(tz)
    calTl.set(new SoftReference[Calendar](instance))
    instance
  }

  def formatTime(long: Long, tz: TimeZone = TimeZone.getDefault, pattern: String = defaultDfPattern): String = {
    val cal = calendarOf(tz)
    cal.setTimeInMillis(long)
    dateFormatOf(tz, pattern).format(cal.getTime)
  }
}
