package chana.collection

import java.util.concurrent.ConcurrentHashMap
import scala.collection.generic.CanBuildFrom
import scala.collection.generic.MutableMapFactory
import scala.collection.mutable.DefaultEntry
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import scala.collection.mutable.MapLike
import scala.collection.JavaConversions._

@SerialVersionUID(1L)
class HashBiMap[A, B](forward: ConcurrentHashMap[A, B], backward: ConcurrentHashMap[B, A]) extends Map[A, B]
    with MapLike[A, B, HashBiMap[A, B]]
    with Serializable {

  def this() = this(new ConcurrentHashMap[A, B](8, 0.9f, 1), new ConcurrentHashMap[B, A](8, 0.9f, 1))

  type Entry = DefaultEntry[A, B]

  private lazy val inverseOne = new HashBiMap(backward, forward)

  def inverse: HashBiMap[B, A] = inverseOne

  override def empty: HashBiMap[A, B] = HashBiMap.empty[A, B]
  override def clear = {
    forward.clear
    backward.clear
  }
  override def size: Int = forward.size

  def get(key: A): Option[B] = {
    Option(forward.get(key))
  }

  override def put(key: A, value: B): Option[B] = {
    backward.put(value, key)
    Option(forward.put(key, value))
  }

  override def update(key: A, value: B): Unit = put(key, value)

  override def remove(key: A): Option[B] = {
    forward.get(key) match {
      case null =>
      case v    => backward.remove(v)
    }
    Option(forward.remove(key))
  }

  def +=(kv: (A, B)): this.type = {
    forward.put(kv._1, kv._2)
    backward.put(kv._2, kv._1)
    this
  }

  def -=(key: A): this.type = {
    forward.get(key) match {
      case null =>
      case v    => backward.remove(v)
    }

    forward.remove(key)
    this
  }

  def iterator = {
    forward.iterator
  }

  override def foreach[C](f: ((A, B)) => C) {
    forward.foreach(f)
  }

  override def keySet: collection.Set[A] = {
    forward.keySet
  }

  override def values: collection.Iterable[B] = {
    forward.values
  }

  override def keysIterator: Iterator[A] = {
    forward.keysIterator
  }

  override def valuesIterator: Iterator[B] = {
    forward.valuesIterator
  }

  private def writeObject(out: java.io.ObjectOutputStream) {
    out.defaultWriteObject
    out.writeObject(forward)
  }

  private def readObject(in: java.io.ObjectInputStream) {
    in.defaultReadObject
    val theForward = in.readObject.asInstanceOf[HashMap[A, B]]
    for ((k, v) <- theForward) {
      forward.put(k, v)
      backward.put(v, k)
    }
  }
}

object HashBiMap extends MutableMapFactory[HashBiMap] {
  implicit def canBuildFrom[A, B]: CanBuildFrom[Coll, (A, B), HashBiMap[A, B]] = new MapCanBuildFrom[A, B]
  def empty[A, B]: HashBiMap[A, B] = new HashBiMap[A, B]
}

