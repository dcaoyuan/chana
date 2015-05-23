package chana.collection

import java.lang.ref.Reference
import java.lang.ref.ReferenceQueue
import java.lang.ref.WeakReference
import scala.collection.generic.CanBuildFrom
import scala.collection.generic.MutableMapFactory
import scala.collection.mutable.DefaultEntry
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import scala.collection.mutable.MapLike

final class WeakValue[A, B](val key: A, val value: B, q: ReferenceQueue[B]) extends WeakReference(value, q) {
  // We need to store the hash code separately since the referent could be removed by the GC.
  private val hash = value.hashCode

  override def hashCode: Int = hash

  override def equals(o: Any): Boolean = {
    if (this eq o.asInstanceOf[AnyRef]) return true

    if (!o.isInstanceOf[WeakValue[A, B]]) return false

    val v1 = get.asInstanceOf[AnyRef]
    val v2 = o.asInstanceOf[WeakValue[A, B]].get.asInstanceOf[AnyRef]

    if ((v1 eq null) || (v2 eq null)) return false
    if (v1 eq v2) return true

    v1.equals(v2)
  }
}

@SerialVersionUID(1L)
final class WeakHashBiMap[A, B](forward: HashMap[A, Reference[B]], backward: HashMap[Reference[B], A]) extends Map[A, B]
    with MapLike[A, B, WeakHashBiMap[A, B]]
    with Serializable {

  def this() = this(new HashMap[A, Reference[B]], new HashMap[Reference[B], A])

  type Entry = DefaultEntry[A, B]

  private lazy val inverseOne = backward

  private val queue = new ReferenceQueue[B]

  def inverse = inverseOne

  override def empty: WeakHashBiMap[A, B] = WeakHashBiMap.empty[A, B]
  override def clear = {
    forward.clear
    backward.clear
  }
  override def size: Int = {
    processRefQueue
    forward.size
  }

  def get(key: A): Option[B] = {
    forward.get(key) match {
      case Some(ref) =>
        val value = ref.get
        if (value == null) {
          forward.remove(key)
          //backward.remove(key)
          None
        } else Some(value)
      case None => None
    }
  }

  override def put(key: A, value: B): Option[B] = {
    processRefQueue
    val ref = new WeakValue(key, value, queue)
    backward.put(ref, key)
    forward.put(key, ref) map (_.get)
  }

  override def update(key: A, value: B): Unit = put(key, value)

  override def remove(key: A): Option[B] = {
    processRefQueue
    forward.get(key) match {
      case Some(v) => backward.remove(v)
      case None    =>
    }
    forward.remove(key) map (_.get)
  }

  def +=(kv: (A, B)): this.type = {
    processRefQueue
    val ref = new WeakValue(kv._1, kv._2, queue)
    forward += (kv._1 -> ref)
    backward += (ref -> kv._1)
    this
  }

  def -=(key: A): this.type = {
    processRefQueue
    forward.get(key) match {
      case Some(v) => backward -= v
      case None    =>
    }

    forward -= key
    this
  }

  def iterator = {
    processRefQueue
    forward.iterator map { x => (x._1, x._2.get) }
  }

  override def foreach[C](f: ((A, B)) => C) {
    processRefQueue
    forward map { x => (x._1, x._2.get) } foreach (f)
  }

  override def keySet: collection.Set[A] = {
    processRefQueue
    forward.keySet
  }

  override def values: collection.Iterable[B] = {
    processRefQueue
    forward.values map (_.get)
  }

  override def keysIterator: Iterator[A] = {
    processRefQueue
    forward.keysIterator
  }

  override def valuesIterator: Iterator[B] = {
    processRefQueue
    forward.valuesIterator map { _.get }
  }

  private def writeObject(out: java.io.ObjectOutputStream) {
    out.defaultWriteObject
    out.writeObject(forward)
  }

  private def readObject(in: java.io.ObjectInputStream) {
    in.defaultReadObject
    val theForward = in.readObject.asInstanceOf[HashMap[A, Reference[B]]]
    for ((k, v) <- theForward) {
      forward += (k -> v)
      backward += (v -> k)
    }
  }

  private def processRefQueue {
    var ref: WeakValue[A, B] = null
    while ({ ref = queue.poll.asInstanceOf[WeakValue[A, B]]; ref != null }) {
      remove(ref.key)
    }
  }
}

object WeakHashBiMap extends MutableMapFactory[WeakHashBiMap] {
  implicit def canBuildFrom[A, B]: CanBuildFrom[Coll, (A, B), WeakHashBiMap[A, B]] = new MapCanBuildFrom[A, B]
  def empty[A, B]: WeakHashBiMap[A, B] = new WeakHashBiMap[A, B]
}

