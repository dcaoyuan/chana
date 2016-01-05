package chana.xpath

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.temporal.Temporal

/**
 * http://www.w3.org/TR/xpath-functions/
 */
object XPathFunctions {

  def map[A, R](xs: java.util.Collection[A])(f: A => R): java.util.Collection[R] = {
    val res = new java.util.ArrayList[R]()
    val itr = xs.iterator

    while (itr.hasNext) {
      res.add(f(itr.next))
    }
    res
  }

  def map[A, B, R](xs: java.util.Collection[A], ys: java.util.Collection[B])(f: (A, B) => R): java.util.Collection[R] = {
    val res = new java.util.ArrayList[R]()
    val itrX = xs.iterator
    val itrY = ys.iterator

    while (itrX.hasNext && itrY.hasNext) {
      res.add(f(itrX.next, itrY.next))
    }
    res
  }

  def plus(left: Any, right: Any) = (left, right) match {
    case (xs: java.util.Collection[_], ys: java.util.Collection[_]) => map(xs, ys)(internal_plus)
    case (xs: java.util.Collection[_], y) => map(xs)(internal_plusRight(y))
    case (x, ys: java.util.Collection[_]) => map(ys)(internal_plusLeft(x))
    case (x, y) => internal_plus(x, y)
  }
  private def internal_plusLeft(left: Any) = { (right: Any) => internal_plus(left, right) }
  private def internal_plusRight(right: Any) = { (left: Any) => internal_plus(left, right) }
  private def internal_plus(left: Any, right: Any): Number = {
    (left.asInstanceOf[AnyRef], right.asInstanceOf[AnyRef]) match {
      case (x: java.lang.Double, y: Number)  => x + y.doubleValue
      case (x: Number, y: java.lang.Double)  => x.doubleValue + y
      case (x: java.lang.Float, y: Number)   => x + y.floatValue
      case (x: Number, y: java.lang.Float)   => x.floatValue + y
      case (x: java.lang.Long, y: Number)    => x + y.longValue
      case (x: Number, y: java.lang.Long)    => x.longValue + y
      case (x: java.lang.Integer, y: Number) => x + y.intValue
      case (x: Number, y: java.lang.Integer) => x.intValue + y
      case x                                 => throw XPathRuntimeException(x, "is not pair of number")
    }
  }

  def minus(left: Any, right: Any) = (left, right) match {
    case (xs: java.util.Collection[_], ys: java.util.Collection[_]) => map(xs, ys)(internal_minus)
    case (xs: java.util.Collection[_], y) => map(xs)(internal_minusRight(y))
    case (x, ys: java.util.Collection[_]) => map(ys)(internal_minusLeft(x))
    case (x, y) => internal_minus(x, y)
  }
  private def internal_minusLeft(left: Any) = { (right: Any) => internal_minus(left, right) }
  private def internal_minusRight(right: Any) = { (left: Any) => internal_minus(left, right) }
  private def internal_minus(left: Any, right: Any): Number = {
    (left.asInstanceOf[AnyRef], right.asInstanceOf[AnyRef]) match {
      case (x: java.lang.Double, y: Number)  => x - y.doubleValue
      case (x: Number, y: java.lang.Double)  => x.doubleValue - y
      case (x: java.lang.Float, y: Number)   => x - y.floatValue
      case (x: Number, y: java.lang.Float)   => x.floatValue - y
      case (x: java.lang.Long, y: Number)    => x - y.longValue
      case (x: Number, y: java.lang.Long)    => x.longValue - y
      case (x: java.lang.Integer, y: Number) => x - y.intValue
      case (x: Number, y: java.lang.Integer) => x.intValue - y
      case x                                 => throw XPathRuntimeException(x, "is not pair of number")
    }
  }

  def multiply(left: Any, right: Any) = (left, right) match {
    case (xs: java.util.Collection[_], ys: java.util.Collection[_]) => map(xs, ys)(internal_multiply)
    case (xs: java.util.Collection[_], y) => map(xs)(internal_multiplyRight(y))
    case (x, ys: java.util.Collection[_]) => map(ys)(internal_multiplyLeft(x))
    case (x, y) => internal_multiply(x, y)
  }
  private def internal_multiplyLeft(left: Any) = { (right: Any) => internal_multiply(left, right) }
  private def internal_multiplyRight(right: Any) = { (left: Any) => internal_multiply(left, right) }
  private def internal_multiply(left: Any, right: Any): Number = {
    (left.asInstanceOf[AnyRef], right.asInstanceOf[AnyRef]) match {
      case (x: java.lang.Double, y: Number)  => x * y.doubleValue
      case (x: Number, y: java.lang.Double)  => x.doubleValue * y
      case (x: java.lang.Float, y: Number)   => x * y.floatValue
      case (x: Number, y: java.lang.Float)   => x.floatValue * y
      case (x: java.lang.Long, y: Number)    => x * y.longValue
      case (x: Number, y: java.lang.Long)    => x.longValue * y
      case (x: java.lang.Integer, y: Number) => x * y.intValue
      case (x: Number, y: java.lang.Integer) => x.intValue * y
      case x                                 => throw XPathRuntimeException(x, "is not pair of number")
    }
  }

  def divide(left: Any, right: Any) = (left, right) match {
    case (xs: java.util.Collection[_], ys: java.util.Collection[_]) => map(xs, ys)(internal_divide)
    case (xs: java.util.Collection[_], y) => map(xs)(internal_divideRight(y))
    case (x, ys: java.util.Collection[_]) => map(ys)(internal_divideLeft(x))
    case (x, y) => internal_divide(x, y)
  }
  private def internal_divideLeft(left: Any) = { (right: Any) => internal_divide(left, right) }
  private def internal_divideRight(right: Any) = { (left: Any) => internal_divide(left, right) }
  private def internal_divide(left: Any, right: Any): Number = {
    (left.asInstanceOf[AnyRef], right.asInstanceOf[AnyRef]) match {
      case (x: java.lang.Double, y: Number)  => x / y.doubleValue
      case (x: Number, y: java.lang.Double)  => x.doubleValue / y
      case (x: java.lang.Float, y: Number)   => x / y.floatValue
      case (x: Number, y: java.lang.Float)   => x.floatValue / y
      case (x: java.lang.Long, y: Number)    => x / y.longValue
      case (x: Number, y: java.lang.Long)    => x.longValue / y
      case (x: java.lang.Integer, y: Number) => x / y.intValue
      case (x: Number, y: java.lang.Integer) => x.intValue / y
      case x                                 => throw XPathRuntimeException(x, "is not pair of number")
    }
  }

  def idivide(left: Any, right: Any) = (left, right) match {
    case (xs: java.util.Collection[_], ys: java.util.Collection[_]) => map(xs, ys)(internal_idivide)
    case (xs: java.util.Collection[_], y) => map(xs)(internal_idivideRight(y))
    case (x, ys: java.util.Collection[_]) => map(ys)(internal_idivideLeft(x))
    case (x, y) => internal_idivide(x, y)
  }
  private def internal_idivideLeft(left: Any) = { (right: Any) => internal_idivide(left, right) }
  private def internal_idivideRight(right: Any) = { (left: Any) => internal_idivide(left, right) }
  private def internal_idivide(left: Any, right: Any): Number = {
    (left.asInstanceOf[AnyRef], right.asInstanceOf[AnyRef]) match {
      case (x: java.lang.Double, y: Number)  => x.intValue / y.intValue
      case (x: Number, y: java.lang.Double)  => x.intValue / y.intValue
      case (x: java.lang.Float, y: Number)   => x.intValue / y.intValue
      case (x: Number, y: java.lang.Float)   => x.intValue / y.intValue
      case (x: java.lang.Long, y: Number)    => x.intValue / y.intValue
      case (x: Number, y: java.lang.Long)    => x.intValue / y.intValue
      case (x: java.lang.Integer, y: Number) => x.intValue / y.intValue
      case (x: Number, y: java.lang.Integer) => x.intValue / y.intValue
      case x                                 => throw XPathRuntimeException(x, "is not pair of number")
    }
  }

  def mod(left: Any, right: Any) = (left, right) match {
    case (xs: java.util.Collection[_], ys: java.util.Collection[_]) => map(xs, ys)(internal_mod)
    case (xs: java.util.Collection[_], y) => map(xs)(internal_modRight(y))
    case (x, ys: java.util.Collection[_]) => map(ys)(internal_modLeft(x))
    case (x, y) => internal_mod(x, y)
  }
  private def internal_modLeft(left: Any) = { (right: Any) => internal_mod(left, right) }
  private def internal_modRight(right: Any) = { (left: Any) => internal_mod(left, right) }
  private def internal_mod(left: Any, right: Any): Number = {
    (left.asInstanceOf[AnyRef], right.asInstanceOf[AnyRef]) match {
      case (x: Number, y: Number) => x.intValue % y.intValue
      case x                      => throw XPathRuntimeException(x, "is not pair of number")
    }
  }

  def neg(v: Any) = v match {
    case xs: java.util.Collection[_] => map(xs)(internal_neg)
    case _                           => internal_neg(v)
  }
  private def internal_neg(v: Any): Number = {
    v match {
      case x: java.lang.Double  => -x
      case x: java.lang.Float   => -x
      case x: java.lang.Long    => -x
      case x: java.lang.Integer => -x
      case x                    => throw XPathRuntimeException(x, "is not a number")
    }
  }

  def abs(v: Any) = v match {
    case xs: java.util.Collection[_] => map(xs)(internal_abs)
    case _                           => internal_abs(v)
  }
  private def internal_abs(v: Any): Number = {
    v.asInstanceOf[AnyRef] match {
      case x: java.lang.Integer => math.abs(x)
      case x: java.lang.Long    => math.abs(x)
      case x: java.lang.Float   => math.abs(x)
      case x: java.lang.Double  => math.abs(x)
      case x                    => throw XPathRuntimeException(x, "is not a number")
    }
  }

  def eq(left: Any, right: Any) = (left, right) match {
    case (xs: java.util.Collection[_], ys: java.util.Collection[_]) => map(xs, ys)(internal_eq)
    case (xs: java.util.Collection[_], y) => map(xs)(internal_eqRight(y))
    case (x, ys: java.util.Collection[_]) => map(ys)(internal_eqLeft(x))
    case (x, y) => internal_eq(x, y)
  }
  private def internal_eqLeft(left: Any) = { (right: Any) => internal_eq(left, right) }
  private def internal_eqRight(right: Any) = { (left: Any) => internal_eq(left, right) }
  private def internal_eq(left: Any, right: Any): Boolean = {
    (left.asInstanceOf[AnyRef], right.asInstanceOf[AnyRef]) match {
      case (x: Number, y: Number) => x == y
      case (x: CharSequence, y: CharSequence) => x == y
      case (x: LocalTime, y: LocalTime) => !(x.isAfter(y) || x.isBefore(y)) // ??
      case (x: LocalDate, y: LocalDate) => x.isEqual(y)
      case (x: LocalDateTime, y: LocalDateTime) => x.isEqual(y)
      case (x: Temporal, y: Temporal) => x == y
      case (x: java.lang.Boolean, y: java.lang.Boolean) => x == y
      case x => throw XPathRuntimeException(x, "can not be applied EQ")
    }
  }

  def ne(left: Any, right: Any) = (left, right) match {
    case (xs: java.util.Collection[_], ys: java.util.Collection[_]) => map(xs, ys)(internal_ne)
    case (xs: java.util.Collection[_], y) => map(xs)(internal_neRight(y))
    case (x, ys: java.util.Collection[_]) => map(ys)(internal_neLeft(x))
    case (x, y) => internal_ne(x, y)
  }
  private def internal_neLeft(left: Any) = { (right: Any) => internal_ne(left, right) }
  private def internal_neRight(right: Any) = { (left: Any) => internal_ne(left, right) }
  private def internal_ne(left: Any, right: Any) = {
    (left.asInstanceOf[AnyRef], right.asInstanceOf[AnyRef]) match {
      case (x: Number, y: Number) => x != y
      case (x: CharSequence, y: CharSequence) => x != y
      case (x: LocalTime, y: LocalTime) => x.isAfter(y) || x.isBefore(y)
      case (x: LocalDate, y: LocalDate) => !x.isEqual(y)
      case (x: LocalDateTime, y: LocalDateTime) => !x.isEqual(y)
      case (x: Temporal, y: Temporal) => x != y
      case (x: java.lang.Boolean, y: java.lang.Boolean) => x != y
      case x => throw XPathRuntimeException(x, "can not be applied NE")
    }
  }

  def gt(left: Any, right: Any) = (left, right) match {
    case (xs: java.util.Collection[_], ys: java.util.Collection[_]) => map(xs, ys)(internal_gt)
    case (xs: java.util.Collection[_], y) => map(xs)(internal_gtRight(y))
    case (x, ys: java.util.Collection[_]) => map(ys)(internal_gtLeft(x))
    case (x, y) => internal_gt(x, y)
  }
  private def internal_gtLeft(left: Any) = { (right: Any) => internal_gt(left, right) }
  private def internal_gtRight(right: Any) = { (left: Any) => internal_gt(left, right) }
  private def internal_gt(left: Any, right: Any) = {
    (left.asInstanceOf[AnyRef], right.asInstanceOf[AnyRef]) match {
      case (x: Number, y: Number)               => x.doubleValue > y.doubleValue
      case (x: LocalTime, y: LocalTime)         => x.isAfter(y)
      case (x: LocalDate, y: LocalDate)         => x.isAfter(y)
      case (x: LocalDateTime, y: LocalDateTime) => x.isAfter(y)
      case x                                    => throw XPathRuntimeException(x, "can not be applied GT")
    }
  }

  def ge(left: Any, right: Any) = (left, right) match {
    case (xs: java.util.Collection[_], ys: java.util.Collection[_]) => map(xs, ys)(internal_ge)
    case (xs: java.util.Collection[_], y) => map(xs)(internal_geRight(y))
    case (x, ys: java.util.Collection[_]) => map(ys)(internal_geLeft(x))
    case (x, y) => internal_ge(x, y)
  }
  private def internal_geLeft(left: Any) = { (right: Any) => internal_ge(left, right) }
  private def internal_geRight(right: Any) = { (left: Any) => internal_ge(left, right) }
  private def internal_ge(left: Any, right: Any) = {
    (left.asInstanceOf[AnyRef], right.asInstanceOf[AnyRef]) match {
      case (x: Number, y: Number)               => x.doubleValue >= y.doubleValue
      case (x: LocalTime, y: LocalTime)         => x.isAfter(y) || !x.isBefore(y)
      case (x: LocalDate, y: LocalDate)         => x.isAfter(y) || x.isEqual(y)
      case (x: LocalDateTime, y: LocalDateTime) => x.isAfter(y) || x.isEqual(y)
      case x                                    => throw XPathRuntimeException(x, "can not be applied GE")
    }
  }

  def lt(left: Any, right: Any) = (left, right) match {
    case (xs: java.util.Collection[_], ys: java.util.Collection[_]) => map(xs, ys)(internal_lt)
    case (xs: java.util.Collection[_], y) => map(xs)(internal_ltRight(y))
    case (x, ys: java.util.Collection[_]) => map(ys)(internal_ltLeft(x))
    case (x, y) => internal_lt(x, y)
  }
  private def internal_ltLeft(left: Any) = { (right: Any) => internal_lt(left, right) }
  private def internal_ltRight(right: Any) = { (left: Any) => internal_lt(left, right) }
  private def internal_lt(left: Any, right: Any) = {
    (left.asInstanceOf[AnyRef], right.asInstanceOf[AnyRef]) match {
      case (x: Number, y: Number)               => x.doubleValue < y.doubleValue
      case (x: LocalTime, y: LocalTime)         => x.isBefore(y)
      case (x: LocalDate, y: LocalDate)         => x.isBefore(y)
      case (x: LocalDateTime, y: LocalDateTime) => x.isBefore(y)
      case x                                    => throw XPathRuntimeException(x, "can not be applied LT")
    }
  }

  def le(left: Any, right: Any) = (left, right) match {
    case (xs: java.util.Collection[_], ys: java.util.Collection[_]) => map(xs, ys)(internal_le)
    case (xs: java.util.Collection[_], y) => map(xs)(internal_leRight(y))
    case (x, ys: java.util.Collection[_]) => map(ys)(internal_leLeft(x))
    case (x, y) => internal_le(x, y)
  }
  private def internal_leLeft(left: Any) = { (right: Any) => internal_le(left, right) }
  private def internal_leRight(right: Any) = { (left: Any) => internal_le(left, right) }
  private def internal_le(left: Any, right: Any) = {
    (left.asInstanceOf[AnyRef], right.asInstanceOf[AnyRef]) match {
      case (x: Number, y: Number)               => x.doubleValue <= y.doubleValue
      case (x: LocalTime, y: LocalTime)         => x.isBefore(y) || !x.isAfter(y)
      case (x: LocalDate, y: LocalDate)         => x.isBefore(y) || x.isEqual(y)
      case (x: LocalDateTime, y: LocalDateTime) => x.isBefore(y) || x.isEqual(y)
      case x                                    => throw XPathRuntimeException(x, "can not be applied LE")
    }
  }

  def and(left: Any, right: Any) = (left, right) match {
    case (xs: java.util.Collection[_], ys: java.util.Collection[_]) => map(xs, ys)(internal_and)
    case (xs: java.util.Collection[_], y) => map(xs)(internal_andRight(y))
    case (x, ys: java.util.Collection[_]) => map(ys)(internal_andLeft(x))
    case (x, y) => internal_and(x, y)
  }
  private def internal_andLeft(left: Any) = { (right: Any) => internal_and(left, right) }
  private def internal_andRight(right: Any) = { (left: Any) => internal_and(left, right) }
  private def internal_and(left: Any, right: Any): Boolean = {
    (left.asInstanceOf[AnyRef], right.asInstanceOf[AnyRef]) match {
      case (x: java.lang.Boolean, y: java.lang.Boolean) => x && y
      case x =>
        throw XPathRuntimeException(x, "can not be applied AND")
    }
  }

  def or(left: Any, right: Any) = (left, right) match {
    case (xs: java.util.Collection[_], ys: java.util.Collection[_]) => map(xs, ys)(internal_or)
    case (xs: java.util.Collection[_], y) => map(xs)(internal_orRight(y))
    case (x, ys: java.util.Collection[_]) => map(ys)(internal_orLeft(x))
    case (x, y) => internal_or(x, y)
  }
  private def internal_orLeft(left: Any) = { (right: Any) => internal_or(left, right) }
  private def internal_orRight(right: Any) = { (left: Any) => internal_or(left, right) }
  private def internal_or(left: Any, right: Any): Boolean = {
    (left.asInstanceOf[AnyRef], right.asInstanceOf[AnyRef]) match {
      case (x: java.lang.Boolean, y: java.lang.Boolean) => x || y
      case x => throw XPathRuntimeException(x, "can not be applied OR")
    }
  }

  def not(v: Any) = v match {
    case xs: java.util.Collection[_] => map(xs)(internal_not)
    case _                           => internal_not(v)
  }
  private def internal_not(v: Any): Boolean = {
    v match {
      case x: java.lang.Boolean => !x
      case x                    => throw XPathRuntimeException(x, "can not be applied NOT")
    }
  }

  def range(left: Any, right: Any): Range = (left, right) match {
    case (x: Number, y: Number) => Range.inclusive(x.intValue, y.intValue)
    case x                      => throw XPathRuntimeException(x, "can not be applied RANGE")
  }

  def strConcat(strs: List[Any]) = {
    strs.mkString
  }

  def strLike(str: String, expr: String, escape: Option[String]): Boolean = {
    val likeExpr = expr.toLowerCase.replace(".", "\\.").replace("?", ".").replace("%", ".*")
    str.toLowerCase.matches(likeExpr)
  }

  def between(base: Any, min: Any, max: Any) = {
    (base.asInstanceOf[AnyRef], min.asInstanceOf[AnyRef], max.asInstanceOf[AnyRef]) match {
      case (x: Number, min: Number, max: Number) =>
        x.doubleValue >= min.doubleValue && x.doubleValue <= max.doubleValue
      case (x: LocalTime, min: LocalTime, max: LocalTime) =>
        (x.isAfter(min) || !x.isBefore(min)) && (x.isBefore(max) || !x.isAfter(max))
      case (x: LocalDate, min: LocalDate, max: LocalDate) =>
        (x.isAfter(min) || x.isEqual(min)) && (x.isBefore(max) || x.isEqual(max))
      case (x: LocalDateTime, min: LocalDateTime, max: LocalDateTime) =>
        (x.isAfter(min) || x.isEqual(min)) && (x.isBefore(max) || x.isEqual(max))
      case x => throw XPathRuntimeException(x, "can not be appled BETWEEN")
    }
  }

  def currentTime() = LocalTime.now()
  def currentDate() = LocalDate.now()
  def currentDateTime() = LocalDateTime.now()

  // ----- functions applicable on arrays

  def last(xs: java.util.Collection[_]): Int = {
    xs.size
  }

  def position(xs: java.util.Collection[_]): java.util.Collection[Int] = {
    val res = new java.util.ArrayList[Int]()
    val itr = xs.iterator
    var i = 1

    while (itr.hasNext) {
      itr.next
      res.add(i)
      i += 1
    }
    res
  }

  def name(xs: java.util.Map[String, _]): java.util.Collection[String] = {
    xs.keySet
  }

}
