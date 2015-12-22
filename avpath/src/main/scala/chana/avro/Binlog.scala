package chana.avro

object Binlog {
  def apply(tpe: Byte, xpath: String, value: Any) = {
    tpe match {
      case -1 => Clearlog(xpath)
      case 0  => Deletelog(xpath, value.asInstanceOf[java.util.Collection[_]])
      case 1  => Changelog(xpath, value.asInstanceOf[Array[Byte]])
      case 2  => Insertlog(xpath, value.asInstanceOf[Array[Byte]])
    }
  }
  def unapply(binlog: Binlog): Option[(String, Any)] = {
    Some((binlog.xpath, binlog.value))
  }
}
sealed trait Binlog extends Serializable {
  def `type`: Byte
  def xpath: String
  def value: Any
}

final case class Clearlog(xpath: String) extends Binlog {
  def `type` = -1
  def value = null
}
final case class Deletelog(xpath: String, value: java.util.Collection[_]) extends Binlog {
  def `type` = 0
}

final case class Changelog(xpath: String, value: Array[Byte]) extends Binlog {
  def `type` = 1

  override def equals(x: Any) = x match {
    case Changelog(xpath, value) => xpath == this.xpath && java.util.Arrays.equals(value, this.value)
    case _                       => false
  }
}
final case class Insertlog(xpath: String, value: Array[Byte]) extends Binlog {
  def `type` = 2

  override def equals(x: Any) = x match {
    case Insertlog(xpath, value) => xpath == this.xpath && java.util.Arrays.equals(value, this.value)
    case _                       => false
  }
}

final case class UpdateAction(commit: () => Any, rollback: () => Any, binlog: Binlog)

final case class UpdateEvent(binlogs: Array[Binlog]) extends Serializable {
  override def equals(o: Any): Boolean = o match {
    case UpdateEvent(binlogs) => this.binlogs.sameElements(binlogs)
    case _                    => false
  }
}
