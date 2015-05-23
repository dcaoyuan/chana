package chana.collection

import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.generic.CanBuildFrom
import scala.collection.generic.MutableMapFactory
import scala.collection.mutable.Map
import scala.collection.mutable.MapLike
import scala.reflect.ClassTag

@SerialVersionUID(1L)
final class WeakIdentityBiHashMap[A, B](protected implicit val m: ClassTag[A]) extends Map[A, B]
    with MapLike[A, B, WeakIdentityBiHashMap[A, B]]
    with WeakIdentityBiHashTable[A, B]
    with Serializable {

  private val readWriteLock = new ReentrantReadWriteLock
  val readLock = readWriteLock.readLock
  val writeLock = readWriteLock.writeLock

  override def empty: WeakIdentityBiHashMap[A, B] = new WeakIdentityBiHashMap[A, B]

  override def clear() = clearTable

  override def size: Int = {
    if (tableSize == 0) return 0
    expungeStaleEntries
    tableSize
  }

  def get(key: A): Option[B] = {
    val e = findEntry(key)
    if (e == null) None
    else Some(e.value)
  }

  def getByValue(value: B): Option[A] = {
    val e = findEntryByValue(value)
    if (e == null) None
    else Some(e.key)
  }

  override def put(key: A, value: B): Option[B] = {
    val e = findEntry(key)

    if (e == null) {
      // make sure: key 1 <--> 1 value
      val e1 = findEntryByValue(value)
      if (e1 == null) {
        addEntry(new WeakEntry(key, value, queue))
        None
      } else {
        removeEntry(e1)
        addEntry(new WeakEntry(key, value, queue))
        Some(e1.value)
      }
    } else {
      val v = e.value
      e.value = value
      Some(v)
    }
  }

  override def update(key: A, value: B): Unit = put(key, value)

  override def remove(key: A): Option[B] = {
    val e = removeEntry(key)
    if (e ne null) Some(e.value)
    else None
  }

  def +=(kv: (A, B)): this.type = {
    val e = findEntry(kv._1)
    if (e == null) addEntry(new WeakEntry(kv._1, kv._2, queue))
    else e.value = kv._2
    this
  }

  def -=(key: A): this.type = { removeEntry(key); this }

  def iterator = entriesIterator map { e => (e.key, e.value) }

  override def foreach[C](f: ((A, B)) => C): Unit = foreachEntry(e => f(e.key, e.value))

  /* Override to avoid tuple allocation in foreach */
  override def keySet: collection.Set[A] = new DefaultKeySet {
    override def foreach[C](f: A => C) = foreachEntry(e => f(e.key))
  }

  /* Override to avoid tuple allocation in foreach */
  override def values: collection.Iterable[B] = new DefaultValuesIterable {
    override def foreach[C](f: B => C) = foreachEntry(e => f(e.value))
  }

  /* Override to avoid tuple allocation */
  override def keysIterator: Iterator[A] = new Iterator[A] {
    val iter = entriesIterator
    def hasNext = iter.hasNext
    def next = iter.next.key
  }

  /* Override to avoid tuple allocation */
  override def valuesIterator: Iterator[B] = new Iterator[B] {
    val iter = entriesIterator
    def hasNext = iter.hasNext
    def next = iter.next.value
  }

  private def writeObject(out: java.io.ObjectOutputStream) {
    serializeTo(out, _.value)
  }

  private def readObject(in: java.io.ObjectInputStream) {
    init[B](in, new WeakEntry(_, _, queue))
  }
}

object WeakIdentityBiHashMap extends MutableMapFactory[WeakIdentityBiHashMap] {
  implicit def canBuildFrom[A, B]: CanBuildFrom[Coll, (A, B), WeakIdentityBiHashMap[A, B]] = new MapCanBuildFrom[A, B]
  def empty[A, B]: WeakIdentityBiHashMap[A, B] = new WeakIdentityBiHashMap[AnyRef, B].asInstanceOf[WeakIdentityBiHashMap[A, B]]
}

