package chana.avro

sealed trait Binlog extends Serializable {
  def `type`: Byte
  def xpath: String
  def value: Any
}

sealed trait BinlogWithEncodedBytes extends Binlog {

  /**
   * Avro encoded bytes of value
   */
  def bytes: Array[Byte]

  /**
   * We'll compare encoded bytes instead of value
   */
  override def equals(x: Any) = x match {
    case that: BinlogWithEncodedBytes => this.`type` == that.`type` && this.xpath == that.xpath && java.util.Arrays.equals(this.bytes, that.bytes)
    case _                            => false
  }
}

final case class Clearlog(xpath: String) extends Binlog {
  def `type` = -1
  def value = ()
}

final case class Deletelog(xpath: String, value: java.util.Collection[_]) extends Binlog {
  def `type` = 0
}

object Changelog {
  def apply(xpath: String, bytes: Array[Byte]) = new Changelog(xpath, (), bytes)
}
final case class Changelog(xpath: String, value: Any, bytes: Array[Byte]) extends BinlogWithEncodedBytes {
  def `type` = 1
}

object Insertlog {
  def apply(xpath: String, bytes: Array[Byte]) = new Insertlog(xpath, (), bytes)
}
final case class Insertlog(xpath: String, value: Any, bytes: Array[Byte]) extends BinlogWithEncodedBytes {
  def `type` = 2
}

final case class UpdateAction(commit: () => Any, rollback: () => Any, binlog: Binlog)

final case class UpdateEvent(binlogs: Array[Binlog]) extends Serializable {
  override def equals(o: Any): Boolean = o match {
    case UpdateEvent(binlogs) => this.binlogs.sameElements(binlogs)
    case _                    => false
  }
}
