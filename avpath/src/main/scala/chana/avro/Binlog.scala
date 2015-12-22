package chana.avro

import org.apache.avro.Schema

object Binlog {
  def apply(tpe: Byte, xpath: String, value: Any, schema: Schema) = {
    tpe match {
      case -1 => Deletelog(xpath, value, schema)
      case 0  => Changelog(xpath, value, schema)
      case 1  => Insertlog(xpath, value, schema)
      case 2  => Clearlog(xpath, value, schema)
    }
  }
  def unapply(binlog: Binlog): Option[(String, Any, Schema)] = {
    Some((binlog.xpath, binlog.value, binlog.schema))
  }
}
sealed trait Binlog extends Serializable {
  def `type`: Byte
  def xpath: String
  def value: Any
  def schema: Schema
}
final case class Deletelog(xpath: String, value: Any, schema: Schema) extends Binlog { def `type` = -1 }
final case class Changelog(xpath: String, value: Any, schema: Schema) extends Binlog { def `type` = 0 }
final case class Insertlog(xpath: String, value: Any, schema: Schema) extends Binlog { def `type` = 1 }
final case class Clearlog(xpath: String, value: Any, schema: Schema) extends Binlog { def `type` = 2 }

final case class UpdateAction(commit: () => Any, rollback: () => Any, binlog: Binlog)

final case class UpdateEvent(binlogs: Array[Binlog]) extends Serializable {
  override def equals(o: Any): Boolean = o match {
    case UpdateEvent(binlogs) => this.binlogs.sameElements(binlogs)
    case _                    => false
  }
}
