package chana.serializer

import akka.actor.ExtendedActorSystem
import akka.serialization.Serializer
import akka.util.ByteString
import chana.avro
import chana.avro.Clearlog
import chana.avro.Changelog
import chana.avro.Deletelog
import chana.avro.Insertlog
import java.nio.ByteOrder

final class BinlogSerializer(system: ExtendedActorSystem) extends Serializer {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  override def identifier: Int = 302668158
  override def includeManifest: Boolean = false

  override def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case binlog @ Clearlog(xpath) =>
      val builder = ByteString.newBuilder

      builder.putByte(binlog.`type`)
      StringSerializer.appendToByteString(builder, xpath)
      builder.result.toArray

    case binlog @ Deletelog(xpath, keys) =>
      val builder = ByteString.newBuilder

      builder.putByte(binlog.`type`)
      StringSerializer.appendToByteString(builder, xpath)
      val itr = keys.iterator
      var keyTpeAdded = false
      while (itr.hasNext) {
        itr.next match {
          case key: String =>
            if (!keyTpeAdded) {
              builder.putByte(0)
              keyTpeAdded = true
            }
            StringSerializer.appendToByteString(builder, key)
          case key: Int =>
            if (!keyTpeAdded) {
              builder.putByte(1)
              keyTpeAdded = true
            }
            builder.putInt(key)
        }
      }

      builder.result.toArray

    case binlog @ Changelog(xpath, bin) =>
      val builder = ByteString.newBuilder

      builder.putByte(binlog.`type`)
      StringSerializer.appendToByteString(builder, xpath)
      builder.putInt(bin.length)
      builder.putBytes(bin)

      builder.result.toArray

    case binlog @ Insertlog(xpath, bin) =>
      val builder = ByteString.newBuilder

      builder.putByte(binlog.`type`)
      StringSerializer.appendToByteString(builder, xpath)
      builder.putInt(bin.length)
      builder.putBytes(bin)

      builder.result.toArray
    case _ =>
      throw new IllegalArgumentException("Can't serialize a non-Binlog message using BinlogSerializer [" + obj + "]")
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val data = ByteString(bytes).iterator

    val tpe = data.getByte
    val xpath = StringSerializer.fromByteIterator(data)

    tpe match {
      case -1 => Clearlog(xpath)

      case 0 =>
        val keys = new java.util.LinkedList[Any]()
        data.getByte match {
          case 0 => // String
            while (data.hasNext) {
              val key = StringSerializer.fromByteIterator(data)
              keys.add(key)
            }
          case 1 => // Int
            while (data.hasNext) {
              val key = data.getInt
              keys.add(key)
            }
        }

        Deletelog(xpath, keys)

      case 1 =>
        val len = data.getInt
        val bin = Array.ofDim[Byte](len)
        data.getBytes(bin)

        Changelog(xpath, bin)

      case 2 =>
        val len = data.getInt
        val bin = Array.ofDim[Byte](len)
        data.getBytes(bin)

        Insertlog(xpath, bin)
    }
  }

}
