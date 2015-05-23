package chana.timeseries.indicator

/**
 * Class for defining indicator's factor
 *
 * @author Caoyuan Deng
 * @Note
 * If you use Factor in indicator, please considerate AbstractIndicator#InnerFactor first
 * which will be added to Indicator's factors automatically when new it.
 */
class Factor(
    var name: String,
    var value: Double,
    var step: Double = 1.0,
    var minValue: Double = Double.MinValue,
    var maxValue: Double = Double.MaxValue) extends Cloneable {

  @inline override final def equals(a: Any) = a match {
    case x: Factor => this.value == x.value
    case _         => false
  }

  @inline override final def hashCode = value.hashCode

  /** this should not be abstract method to get scalac knowing it's an override of @cloneable instead of java.lang.Object#clone */
  override def clone: Factor = new Factor(this.name, this.value, this.step, this.minValue, this.maxValue)
}

case object FactorChanged
