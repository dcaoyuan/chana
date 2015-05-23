package chana.timeseries

import scala.reflect.ClassTag

/**
 *
 * @author Caoyuan Deng
 */
object Null {
  val Byte = scala.Byte.MinValue // -128 to 127
  val Short = scala.Short.MinValue // -32768 to 32767
  val Char = scala.Char.MinValue // 0(\u0000) to 65535(\uffff)
  val Int = scala.Int.MinValue // -2,147,483,648 to 2,147,483,647
  val Long = scala.Long.MinValue // -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807
  val Float = scala.Float.NaN
  val Double = scala.Double.NaN
  val Boolean = false

  def is(v: Boolean) = v == Boolean
  def is(v: Byte) = v == Byte
  def is(v: Short) = v == Short
  def is(v: Char) = v == Char
  def is(v: Int) = v == Int
  def is(v: Long) = v == Long
  def is(v: Float) = v.isNaN
  def is(v: Double) = v.isNaN
  def is(v: AnyRef) = v eq null

  def not(v: Boolean) = v != Boolean
  def not(v: Byte) = v != Byte
  def not(v: Short) = v != Short
  def not(v: Char) = v != Char
  def not(v: Int) = v != Int
  def not(v: Long) = v != Long
  def not(v: Float) = !v.isNaN
  def not(v: Double) = !v.isNaN
  def not(v: AnyRef) = v ne null

  def value[T: ClassTag]: T = {
    (reflect.classTag[T] match {
      case ClassTag.Boolean => Boolean
      case ClassTag.Byte    => Byte
      case ClassTag.Short   => Short
      case ClassTag.Char    => Char
      case ClassTag.Int     => Int
      case ClassTag.Long    => Long
      case ClassTag.Float   => Float
      case ClassTag.Double  => Double
      case _                => null
    }).asInstanceOf[T]
  }

  // --- simple test
  def main(args: Array[String]) {
    println(value[Boolean] + " type: " + value[Boolean].getClass)
    println(value[Byte] + " type: " + value[Byte].getClass)
    println(value[Short] + " type: " + value[Short].getClass)
    println(value[Char] + " type: " + value[Char].getClass)
    println(value[Int] + " type: " + value[Int].getClass)
    println(value[Long] + " type: " + value[Long].getClass)
    println(value[Float] + " type: " + value[Float].getClass)
    println(value[Double] + " type: " + value[Double].getClass)
    println(value[String])
    println(is(scala.Double.NaN))
    println(is(java.lang.Double.NaN))
  }
}
