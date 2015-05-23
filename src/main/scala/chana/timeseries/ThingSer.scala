package chana.timeseries

import chana.Thing
import scala.collection.mutable

/**
 *
 * @author Caoyuan Deng
 */
abstract class ThingSer(_thing: Thing, _freq: TFreq) extends DefaultTBaseSer(_thing, _freq) {

  private var _shortName: String = thing.identifier
  private var _isAdjusted: Boolean = false

  val open = TVar[Double]("O")
  val high = TVar[Double]("H")
  val low = TVar[Double]("L")
  val close = TVar[Double]("C")
  val volume = TVar[Double]("V")
  val amount = TVar[Double]("A")
  val isClosed = TVar[Boolean]("E")

  override val exportableVars = List(open, high, low, close, volume, amount)
}
