package chana.timeseries

trait Flag {
  import Flag._

  /** dafault could be set to 1, which is closed_! */
  def flag: Int
  def flag_=(flag: Int)

  def closed_? : Boolean = (flag & MaskClosed) == MaskClosed
  def closed_! { flag |= MaskClosed }
  def unclosed_! { flag &= ~MaskClosed }

  def justOpen_? : Boolean = (flag & MaskJustOpen) == MaskJustOpen
  def justOpen_! { flag |= MaskJustOpen }
  def unjustOpen_! { flag &= ~MaskJustOpen }

  /**
   * is this value created/composed by me or loaded from remote or other source
   */
  def fromMe_? : Boolean = (flag & MaskFromMe) == MaskFromMe
  def fromMe_! { flag |= MaskFromMe }
  def unfromMe_! { flag &= ~MaskFromMe }

}

object Flag {
  // bit masks for flag
  val MaskClosed = 1 << 0 // 1   2^^0    000...00000001
  val MaskVerified = 1 << 1 // 2   2^^1    000...00000010
  val MaskFromMe = 1 << 2 // 4   2^^2    000...00000100
  val flagbit4 = 1 << 3 // 8   2^^3    000...00001000
  val flagbit5 = 1 << 4 // 16  2^^4    000...00010000
  val flagbit6 = 1 << 5 // 32  2^^5    000...00100000
  val flagbit7 = 1 << 6 // 64  2^^6    000...01000000
  val MaskJustOpen = 1 << 7 // 128 2^^7    000...10000000
}