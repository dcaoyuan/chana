package chana.reactor

import scala.collection.mutable
import scala.ref._

trait SingleRefCollection[+A <: AnyRef] extends Iterable[A] { self =>

  trait Ref[+A <: AnyRef] extends Reference[A] {
    override def hashCode() = {
      get match {
        case None    => 0
        case Some(x) => x.##
      }
    }
    override def equals(that: Any) = that match {
      case that: ReferenceWrapper[_] =>
        val v1 = this.get
        val v2 = that.get
        v1 == v2
      case _ => false
    }
  }

  //type Ref <: Reference[A] // TODO: could use higher kinded types, but currently crashes
  protected[this] def Ref(a: A): Ref[A]
  protected[this] val referenceQueue = new ReferenceQueue[A]

  protected val underlying: Iterable[Reference[A]]

  def purgeReferences() {
    var ref = referenceQueue.poll
    while (ref != None) {
      removeReference(ref.get.asInstanceOf[Reference[A]])
      ref = referenceQueue.poll
    }
  }

  protected[this] def removeReference(ref: Reference[A])

  def iterator = new Iterator[A] {
    private val elems = self.underlying.iterator
    private var hd: A = _
    private var ahead: Boolean = false
    private def skip: Unit =
      while (!ahead && elems.hasNext) {
        // make sure we have a reference to the next element,
        // otherwise it might be garbage collected
        val nextValue = elems.next
        ahead = nextValue != null
        if (ahead) {
          val next = nextValue.get
          ahead = next != None
          if (ahead) hd = next.get
        }
      }
    def hasNext: Boolean = { skip; ahead }
    def next(): A =
      if (hasNext) { ahead = false; hd }
      else throw new NoSuchElementException("next on empty iterator")
  }
}

class StrongReference[+T <: AnyRef](value: T) extends Reference[T] {
  private[this] var ref: Option[T] = Some(value)
  def isValid: Boolean = ref != None
  def apply(): T = ref.get
  def get: Option[T] = ref
  def clear() { ref = None }
  def enqueue(): Boolean = false
  def isEnqueued(): Boolean = false

  override def toString = get.map(_.toString).getOrElse("<deleted>")
}

abstract class RefBuffer[A <: AnyRef] extends mutable.Buffer[A] with SingleRefCollection[A] { self =>
  protected val underlying: mutable.Buffer[Reference[A]]

  def +=(el: A): this.type = { purgeReferences(); underlying += Ref(el); this }
  def +=:(el: A) = { purgeReferences(); Ref(el) +=: underlying; this }
  def remove(el: A) { underlying -= Ref(el); purgeReferences(); }
  def remove(n: Int) = { val el = apply(n); remove(el); el }
  def insertAll(n: Int, iter: Iterable[A]) {
    purgeReferences()
    underlying.insertAll(n, iter.view.map(Ref(_)))
  }
  def update(n: Int, el: A) { purgeReferences(); underlying(n) = Ref(el) }
  def apply(n: Int) = {
    purgeReferences()
    var el = underlying(n).get
    while (el == None) {
      purgeReferences(); el = underlying(n).get
    }
    el.get
  }

  def length = { purgeReferences(); underlying.length }
  def clear() { underlying.clear(); purgeReferences() }

  protected[this] def removeReference(ref: Reference[A]) { underlying -= ref }
}

abstract class RefSet[A <: AnyRef] extends mutable.Set[A] with SingleRefCollection[A] { self =>
  protected val underlying: mutable.Set[Reference[A]]

  def -=(el: A): this.type = { underlying -= Ref(el); purgeReferences(); this }
  def +=(el: A): this.type = { purgeReferences(); underlying += Ref(el); this }
  def contains(el: A): Boolean = { purgeReferences(); underlying.contains(Ref(el)) }

  override def size = { purgeReferences(); underlying.size }

  protected[this] def removeReference(ref: Reference[A]) { underlying -= ref }
}

