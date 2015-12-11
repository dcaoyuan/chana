package chana.xpath

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.temporal.Temporal

object XPathFunctions {

  type FunctionOnArray = (Any => Any) => List[Any]

  def plus(left: Any, right: Any) = left match {
    case xs: List[_]                    => xs map internal_plusRight(right)
    case fn: FunctionOnArray @unchecked => fn(internal_plusRight(right))
    case _ =>
      right match {
        case xs: List[_]                    => xs map internal_plusLeft(left)
        case fn: FunctionOnArray @unchecked => fn(internal_plusLeft(left))
        case _                              => internal_plus(left, right)
      }
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

  def minus(left: Any, right: Any) = left match {
    case xs: List[_]                    => xs map internal_minusRight(right)
    case fn: FunctionOnArray @unchecked => fn(internal_minusRight(right))
    case _ =>
      right match {
        case xs: List[_]                    => xs map internal_minusLeft(left)
        case fn: FunctionOnArray @unchecked => fn(internal_minusLeft(left))
        case _                              => internal_minus(left, right)
      }
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

  def multiply(left: Any, right: Any) = left match {
    case xs: List[_]                    => xs map internal_multiplyRight(right)
    case fn: FunctionOnArray @unchecked => fn(internal_multiplyRight(right))
    case _ =>
      right match {
        case xs: List[_]                    => xs map internal_multiplyLeft(left)
        case fn: FunctionOnArray @unchecked => fn(internal_multiplyLeft(left))
        case _                              => internal_multiply(left, right)
      }
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

  def divide(left: Any, right: Any) = left match {
    case xs: List[_]                    => xs map internal_divideRight(right)
    case fn: FunctionOnArray @unchecked => fn(internal_divideRight(right))
    case _ =>
      right match {
        case xs: List[_]                    => xs map internal_divideLeft(left)
        case fn: FunctionOnArray @unchecked => fn(internal_divideLeft(left))
        case _                              => internal_divide(left, right)
      }
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

  def idivide(left: Any, right: Any) = left match {
    case xs: List[_]                    => xs map internal_idivideRight(right)
    case fn: FunctionOnArray @unchecked => fn(internal_idivideRight(right))
    case _ =>
      right match {
        case xs: List[_]                    => xs map internal_idivideLeft(left)
        case fn: FunctionOnArray @unchecked => fn(internal_idivideLeft(left))
        case _                              => internal_idivide(left, right)
      }
  }
  private def internal_idivideLeft(left: Any) = { (right: Any) => internal_minus(left, right) }
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

  def mod(left: Any, right: Any) = left match {
    case xs: List[_]                    => xs map internal_modRight(right)
    case fn: FunctionOnArray @unchecked => fn(internal_modRight(right))
    case _ =>
      right match {
        case xs: List[_]                    => xs map internal_modLeft(left)
        case fn: FunctionOnArray @unchecked => fn(internal_modLeft(left))
        case _                              => internal_mod(left, right)
      }
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
    case xs: List[_]                    => xs map internal_neg
    case fn: FunctionOnArray @unchecked => fn(internal_neg)
    case _                              => internal_neg(v)
  }
  def internal_neg(v: Any): Number = {
    v match {
      case x: java.lang.Double  => -x
      case x: java.lang.Float   => -x
      case x: java.lang.Long    => -x
      case x: java.lang.Integer => -x
      case x                    => throw XPathRuntimeException(x, "is not a number")
    }
  }

  def abs(v: Any) = v match {
    case xs: List[_]                    => xs map internal_abs
    case fn: FunctionOnArray @unchecked => fn(internal_abs)
    case _                              => internal_abs(v)
  }
  def internal_abs(v: Any): Number = {
    v.asInstanceOf[AnyRef] match {
      case x: java.lang.Integer => math.abs(x)
      case x: java.lang.Long    => math.abs(x)
      case x: java.lang.Float   => math.abs(x)
      case x: java.lang.Double  => math.abs(x)
      case x                    => throw XPathRuntimeException(x, "is not a number")
    }
  }

  def eq(left: Any, right: Any) = left match {
    case xs: List[_]                    => xs map internal_eqRight(right)
    case fn: FunctionOnArray @unchecked => fn(internal_eqRight(right))
    case _ =>
      right match {
        case xs: List[_]                    => xs map internal_eqLeft(left)
        case fn: FunctionOnArray @unchecked => fn(internal_eqLeft(left))
        case _                              => internal_eq(left, right)
      }
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

  def ne(left: Any, right: Any) = left match {
    case xs: List[_]                    => xs map internal_neRight(right)
    case fn: FunctionOnArray @unchecked => fn(internal_neRight(right))
    case _ =>
      right match {
        case xs: List[_]                    => xs map internal_neLeft(left)
        case fn: FunctionOnArray @unchecked => fn(internal_neLeft(left))
        case _                              => internal_ne(left, right)
      }
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

  def gt(left: Any, right: Any) = left match {
    case xs: List[_]                    => xs map internal_gtRight(right)
    case fn: FunctionOnArray @unchecked => fn(internal_gtRight(right))
    case _ =>
      right match {
        case xs: List[_]                    => xs map internal_gtLeft(left)
        case fn: FunctionOnArray @unchecked => fn(internal_gtLeft(right))
        case _                              => internal_gt(left, right)
      }
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

  def ge(left: Any, right: Any) = left match {
    case xs: List[_]                    => xs map internal_geRight(right)
    case fn: FunctionOnArray @unchecked => fn(internal_geRight(right))
    case _ =>
      right match {
        case xs: List[_]                    => xs map internal_geLeft(left)
        case fn: FunctionOnArray @unchecked => fn(internal_geLeft(left))
        case _                              => internal_ge(left, right)
      }
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

  def lt(left: Any, right: Any) = left match {
    case xs: List[_]                    => xs map internal_ltRight(right)
    case fn: FunctionOnArray @unchecked => fn(internal_ltRight(right))
    case _ =>
      right match {
        case xs: List[_]                    => xs map internal_ltLeft(left)
        case fn: FunctionOnArray @unchecked => fn(internal_ltLeft(left))
        case _                              => internal_lt(left, right)
      }
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

  def le(left: Any, right: Any) = left match {
    case xs: List[_]                    => xs map internal_leRight(right)
    case fn: FunctionOnArray @unchecked => fn(internal_leRight(right))
    case _ =>
      right match {
        case xs: List[_]                    => xs map internal_leLeft(left)
        case fn: FunctionOnArray @unchecked => fn(internal_leLeft(left))
        case _                              => internal_le(left, right)
      }
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

  // ----- functions application on valus

  def last(xs: java.util.Collection[Any]): Int = {
    xs.size
  }

  def position(xs: java.util.Collection[Any]): FunctionOnArray = { (op: Int => Any) =>
    var res = List[Any]()
    val itr = xs.iterator
    var i = 1
    while (itr.hasNext) {
      itr.next
      res ::= op(i)
      i += 1
    }
    res.reverse
  }
}
