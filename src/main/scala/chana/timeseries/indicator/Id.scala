package chana.timeseries.indicator

/**
 * @author Caoyuan Deng
 * @Note ref should implement proper hashCode and equals method,
 *       it could be baseSer or name string etc
 */

import chana.timeseries.TSer
import chana.timeseries.TVar

final class Id[T](val klass: Class[T], val keyRef: AnyRef, val args: Any*) {

  @inline override def equals(o: Any): Boolean = {
    o match {
      case Id(klass, keyRef, args @ _*) if (this.klass.getName == klass.getName) &&
        ((this.keyRef eq keyRef) || (this.keyRef.isInstanceOf[String] && this.keyRef == keyRef)) &&
        (this.args.size == args.size) =>
        val itr1 = this.args.iterator
        val itr2 = args.iterator
        while (itr1.hasNext && itr2.hasNext) {
          val arg1 = itr1.next
          val arg2 = itr2.next
          if (arg1.isInstanceOf[TVar[_]] || arg1.isInstanceOf[TSer] || arg2.isInstanceOf[TVar[_]] || arg2.isInstanceOf[TSer]) {
            if (arg1.asInstanceOf[AnyRef] ne arg2.asInstanceOf[AnyRef]) {
              return false
            }
          } else if (itr1.next != itr2.next) {
            return false
          }
        }
        true
      case _ => false
    }
  }

  @inline override def hashCode: Int = {
    var h = 17
    h = 37 * h + klass.hashCode
    h = 37 * h + keyRef.hashCode
    val itr = args.iterator
    while (itr.hasNext) {
      val more: Int = itr.next match {
        case x: Short   => x
        case x: Char    => x
        case x: Byte    => x
        case x: Int     => x
        case x: Boolean => if (x) 0 else 1
        case x: Long    => (x ^ (x >>> 32)).toInt
        case x: Float   => java.lang.Float.floatToIntBits(x)
        case x: Double =>
          val x1 = java.lang.Double.doubleToLongBits(x)
          (x1 ^ (x1 >>> 32)).toInt
        case x: AnyRef => x.hashCode
      }
      h = 37 * h + more
    }
    h
  }

  def keyString = "(" + klass.getSimpleName + "," + keyRef + "," + args.mkString(",") + ")"

  override def toString = "Id(" + klass.getName + "," + keyRef + "," + args.mkString(",") + ")"
}

object Id {
  def apply[T](klass: Class[T], keyRef: AnyRef, args: Any*) = new Id(klass, keyRef, args: _*)
  def unapplySeq[T](e: Id[T]): Option[(Class[T], AnyRef, Seq[Any])] = Some((e.klass, e.keyRef, e.args))

  // simple test
  def main(args: Array[String]) {
    val keya = "abc"
    val keyb = "abc"

    val id1 = Id(classOf[String], keya, 1)
    val id2 = Id(classOf[String], keyb, 1)
    println(id1 == id2)

    val id3 = Id(classOf[String], keya)
    val id4 = Id(classOf[String], keyb)
    println(id3 == id4)

    val id5 = Id(classOf[Indicator], keya, chana.timeseries.TFreq.ONE_MIN)
    val id6 = Id(classOf[Indicator], keyb, chana.timeseries.TFreq.withName("1m").get)
    println(id5 == id6)
  }
}
