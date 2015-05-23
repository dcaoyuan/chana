package chana.timeseries.indicator

import chana.timeseries.TSer

trait Function extends TSer {

  /**
   * set the function's arguments.
   * @param baseSer, the ser that this function is based, ie. used to compute
   */
  def set(args: Any*)

  /**
   * This method will compute from computedIdx <b>to</b> idx.
   *
   * and AbstractIndicator.compute(final long begTime) will compute <b>from</b>
   * begTime to last data
   *
   * @param sessionId, the sessionId usally is controlled by outside caller,
   *        such as an indicator
   * @param idx, the idx to be computed to
   */
  def computeTo(sessionId: Long, idx: Int)
}
