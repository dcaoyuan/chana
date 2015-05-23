package chana.timeseries.indicator

import akka.actor.Actor
import chana.timeseries.TBaseSer
import chana.timeseries.TSer
import java.text.DecimalFormat

/**
 *
 * @author Caoyuan Deng
 */
object Indicator {

  private val FAC_DECIMAL_FORMAT = new DecimalFormat("0.###")

  def displayName(ser: TSer): String = ser match {
    case x: Indicator => displayName(ser.shortName, x.factors)
    case _            => ser.shortName
  }

  def displayName(name: String, factors: Array[Factor]): String = {
    if (factors.length == 0) {
      name
    } else {
      factors map { x => FAC_DECIMAL_FORMAT.format(x.value) } mkString (name + "(", ",", ")")
    }
  }
}

trait Indicator extends TSer with WithFactors with Ordered[Indicator] {

  protected val Plot = chana.timeseries.indicator.Plot

  protected def indicatorBehavior: Actor.Receive = {
    case ComputeFrom(time) =>
      if (baseSer != null) computeFrom(time)
    case FactorChanged =>
      if (baseSer != null) computeFrom(0)
  }

  reactions += indicatorBehavior

  def baseSer: TBaseSer

  /**
   * If identifier.isDefined, means the baseSer may belong to another one which is of this identifier
   */
  def identifier: Option[String]
  def identifier_=(identifier: String)

  /**
   * @param time to be computed from
   */
  def computeFrom(time: Long)
  def computedTime: Long

  def compare(another: Indicator): Int = {
    if (this.shortName.equalsIgnoreCase(another.shortName)) {
      if (this.hashCode < another.hashCode) -1 else (if (this.hashCode == another.hashCode) 0 else 1)
    } else {
      this.shortName.compareTo(another.shortName)
    }
  }

}

trait WithFactors { _: Indicator =>

  /**
   * factors of this instance, such as period long, period short etc,
   */
  private var _factors = Array[Factor]()

  def factorValues: Array[Double] = factors map { _.value }
  /**
   * if any value of factors changed, will publish FactorChanged
   */
  def factorValues_=(values: Array[Double]) {
    var valueChanged = false
    if (values != null) {
      if (factors.length == values.length) {
        var i = 0
        while (i < values.length) {
          val myFactor = _factors(i)
          val inValue = values(i)
          /** check if changed happens before set myFactor */
          if (myFactor.value != inValue) {
            valueChanged = true
          }
          myFactor.value = inValue
          i += 1
        }
      }
    }

    if (valueChanged) publish(FactorChanged)
  }

  def factors = _factors
  def factors_=(factors: Array[Factor]) {
    if (factors != null) {
      val values = new Array[Double](factors.length)
      var i = 0
      while (i < factors.length) {
        values(i) = factors(i).value
      }
      factorValues = values
      i += 1
    }
  }

  def replaceFactor(oldFactor: Factor, newFactor: Factor) {
    var idxOld = -1
    var i = 0
    var break = false
    while (i < factors.length && !break) {
      val factor = factors(i)
      if (factor == oldFactor) {
        idxOld = i
        break = true
      }
      i += 1
    }

    if (idxOld != -1) {
      factors(idxOld) = newFactor
    }
  }

  private def addFactor(factor: Factor) {
    /** add factor reaction to this factor */
    val olds = _factors
    _factors = new Array[Factor](olds.length + 1)
    System.arraycopy(olds, 0, _factors, 0, olds.length)
    _factors(_factors.length - 1) = factor
  }

  /**
   * Inner Fac class that will be added to AbstractIndicator instance
   * automaticlly when new it.
   * Fac can only lives in AbstractIndicator
   *
   *
   * @see addFactor(..)
   * --------------------------------------------------------------------
   */
  final protected class InnerFactor(
      _name: String,
      _value: Double,
      _step: Double,
      _minValue: Double,
      _maxValue: Double) extends Factor(_name, _value, _step, _minValue, _maxValue) {
    addFactor(this)
  }

  object Factor {
    def apply(name: String, value: Double) = new InnerFactor(name, value, 1.0, Double.MinValue, Double.MaxValue)
    def apply(name: String, value: Double, step: Double) = new InnerFactor(name, value, step, Double.MinValue, Double.MaxValue)
    def apply(name: String, value: Double, step: Double, minValue: Double, maxValue: Double) = new InnerFactor(name, value, step, minValue, maxValue)
  }
}

final case class ComputeFrom(time: Long)
