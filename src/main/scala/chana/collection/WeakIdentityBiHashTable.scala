package chana.collection

import java.lang.ref.ReferenceQueue
import java.lang.ref.WeakReference
import scala.collection.mutable.HashMap
import scala.reflect.ClassTag
import scala.runtime.BoxesRunTime

/**
 * This class can be used to construct data structures that are based
 *  on hashtables. Class `HashTable[K]` implements a hashtable
 *  that maps keys of type `K` to values of the fully abstract
 *  member type `WeakEntry`. Classes that make use of `HashTable`
 *  have to provide an implementation for `WeakEntry`.
 *
 *  There are mainly two parameters that affect the performance of a hashtable:
 *  the <i>initial size</i> and the <i>load factor</i>. The <i>size</i>
 *  refers to the number of <i>buckets</i> in the hashtable, and the <i>load
 *  factor</i> is a measure of how full the hashtable is allowed to get before
 *  its size is automatically doubled. Both parameters may be changed by
 *  overriding the corresponding values in class `HashTable`.
 *
 *  The entries in this hash table extend WeakReference, using its main ref
 *  field as the key.
 *
 *  @author  Matthias Zenger
 *  @author  Martin Odersky
 *  @version 2.0, 31/12/2006
 *  @since   1
 *
 *  @tparam K     type of the elements contained in this hash table.
 */
trait WeakIdentityBiHashTable[K, V] {
  import WeakIdentityBiHashTable._

  class WeakEntry[K, V](private var _key: K, private var _value: V, queue: ReferenceQueue[K]) extends WeakReference[K](_key, queue) {

    /**
     * Scala's primary constructor cause instance to hold a strong reference _key,
     * so should set it to null at once
     */
    _key = null.asInstanceOf[K]

    /**
     * Should remember key's hash, so even when key is GCed, the expungeStaleEntries
     * can get back the index via hash
     */
    var hash: Int = _

    // WeakReference has a private 'next' ?, have to name here nextEntry
    var nextEntry: WeakEntry[K, V] = _

    def key: K = get

    def value = _value
    def value_=(newValue: V) {
      _value = newValue
    }

    override def equals(that: Any): Boolean = {
      if (!that.isInstanceOf[WeakEntry[K, V]]) return false

      val e = that.asInstanceOf[WeakEntry[K, V]]
      val k1 = key.asInstanceOf[AnyRef]
      val k2 = e.key.asInstanceOf[AnyRef]
      if (k1.eq(k2) || (k1 != null && k1.equals(k2))) {
        val v1 = value.asInstanceOf[AnyRef]
        val v2 = e.value.asInstanceOf[AnyRef]
        if (v1.eq(v2) || (v1 != null && v1.equals(v2))) return true
      }

      false
    }

    override def hashCode: Int = {
      val k = key
      val v = value
      ((if (k == null) 0 else k.hashCode) ^
        (if (v == null) 0 else v.hashCode))
    }

    override def toString = {
      key + "=" + value
    }
  }

  protected implicit val m: ClassTag[K]

  /**
   * The load factor for the hash table (in 0.001 step).
   */
  protected def loadFactor: Int = 750 // corresponds to 75%
  protected final val loadFactorDenum = 1000

  /**
   * The initial size of the hash table.
   */
  protected def initialSize: Int = 16

  /**
   * The initial threshold
   */
  protected def initialThreshold: Int = newThreshold(initialCapacity)

  @transient private var _loadFactor = loadFactor

  /**
   * The actual hash table.
   */
  @transient protected var table: Array[WeakEntry[K, V]] = new Array(initialCapacity)

  /**
   * Table store key index of value, searchable according to value's hash
   */
  @transient protected var valueToEntry: HashMap[V, WeakEntry[K, V]] = new HashMap

  /**
   * The number of mappings contained in this hash table.
   */
  @transient protected var tableSize: Int = 0

  /**
   * The next size value at which to resize (capacity * load factor).
   */
  @transient protected var threshold: Int = initialThreshold

  /**
   * Reference queue for cleared WeakEntries
   */
  @transient protected val queue = new ReferenceQueue[K]

  private def initialCapacity = capacity(initialSize)

  /**
   * Initializes the collection from the input stream. `f` will be called for each key/value pair
   * read from the input stream in the order determined by the stream. This is useful for
   * structures where iteration order is important (e.g. LinkedHashMap).
   */
  protected def init[B](in: java.io.ObjectInputStream, f: (K, B) => WeakEntry[K, V]) {
    in.defaultReadObject

    _loadFactor = in.readInt
    assert(_loadFactor > 0)

    val size = in.readInt
    assert(size >= 0)

    table = new Array(capacity(size * loadFactorDenum / _loadFactor))
    valueToEntry = new HashMap
    threshold = newThreshold(table.length)

    var idx = 0
    while (idx < size) {
      addEntry(f(in.readObject.asInstanceOf[K], in.readObject.asInstanceOf[B]))
      idx += 1
    }
  }

  /**
   * Serializes the collection to the output stream by saving the load factor, collection
   * size, collection keys and collection values. `value` is responsible for providing a value
   * from an entry.
   *
   * `foreach` determines the order in which the key/value pairs are saved to the stream. To
   * deserialize, `init` should be used.
   */
  protected def serializeTo[B](out: java.io.ObjectOutputStream, value: WeakEntry[K, V] => B) {
    out.defaultWriteObject
    out.writeInt(loadFactor)
    out.writeInt(tableSize)
    foreachEntry { entry =>
      out.writeObject(entry.key)
      out.writeObject(value(entry))
    }
  }

  private def capacity(expectedSize: Int) = if (expectedSize == 0) 1 else powerOfTwo(expectedSize)

  /**
   * Find entry with given key in table, null if not found.
   */
  protected def findEntry(key: K): WeakEntry[K, V] = {
    val tab = getTable
    val idx = index(elemHashCode(key))
    var e = tab(idx)
    while (e != null && !elemEquals(e.get, key)) e = e.nextEntry
    e
  }

  protected def findEntryByValue(value: V): WeakEntry[K, V] =
    valueToEntry.get(value) getOrElse null

  /**
   * Add entry to table
   *  pre: no entry with same key exists
   */
  protected def addEntry(e: WeakEntry[K, V]) {
    val tab = getTable
    val h = elemHashCode(e.key)
    val idx = index(h)
    e.hash = h
    e.nextEntry = tab(idx)
    tab(idx) = e
    valueToEntry.put(e.value, e)
    tableSize = tableSize + 1
    if (tableSize > threshold)
      resize(2 * tab.length)
  }

  /**
   * Remove entry from table if present.
   */
  protected def removeEntry(key: K): WeakEntry[K, V] = {
    val tab = getTable
    val idx = index(elemHashCode(key))
    var e = tab(idx)
    if (e != null) {
      if (elemEquals(e.get, key)) {
        tab(idx) = e.nextEntry
        tableSize = tableSize - 1
        valueToEntry.remove(e.value)
        return e
      } else {
        var e1 = e.nextEntry
        while (e1 != null && !elemEquals(e1.get, key)) {
          e = e1
          e1 = e1.nextEntry
        }
        if (e1 != null) {
          e.nextEntry = e1.nextEntry
          tableSize = tableSize - 1
          valueToEntry.remove(e1.value)
          return e1
        }
      }
    }
    null
  }

  protected def removeEntry(entry: WeakEntry[K, V]): WeakEntry[K, V] = {
    val tab = getTable
    val idx = index(entry.hash)
    val key = entry.get
    var e = tab(idx)
    if (e != null) {
      if (elemEquals(e.get, key)) {
        tab(idx) = e.nextEntry
        tableSize = tableSize - 1
        valueToEntry.remove(e.value)
        return e
      } else {
        var e1 = e.nextEntry
        while (e1 != null && !elemEquals(e1.get, key)) {
          e = e1
          e1 = e1.nextEntry
        }
        if (e1 != null) {
          e.nextEntry = e1.nextEntry
          tableSize = tableSize - 1
          valueToEntry.remove(e1.value)
          return e1
        }
      }
    }
    null
  }

  /**
   * An iterator returning all entries.
   */
  protected def entriesIterator: Iterator[WeakEntry[K, V]] = new HashIterator[WeakEntry[K, V]] {
    def next: WeakEntry[K, V] = nextEntry
  }

  /* new Iterator[WeakEntry[K, V]] {
   val iterTable = table
   var idx = iterTable.length - 1
   var es = iterTable(idx)
   scan()
   def hasNext = es != null
   def next = {
   val res = es
   es = es.nextEntry
   scan()
   res
   }
   def scan() {
   while (es == null && idx > 0) {
   idx = idx - 1
   es = iterTable(idx)
   }
   }
   } */

  private abstract class HashIterator[T] extends Iterator[T] {
    var idx = table.length - 1
    var entry: WeakEntry[K, V] = _
    var lastReturned: WeakEntry[K, V] = _

    /**
     * Strong reference needed to avoid disappearance of key
     * between hasNext and next
     */
    var nextKey: K = _

    /**
     * Strong reference needed to avoid disappearance of key
     * between nextEntry() and any use of the entry
     */
    var currKey: K = _

    def hasNext: Boolean = {
      val tab = table

      while (nextKey == null) {
        var e = entry
        var i = idx
        while (e == null && i >= 0) {
          e = tab(i)
          i -= 1
        }
        entry = e
        idx = i
        if (e == null) {
          currKey = null.asInstanceOf[K]
          return false
        }
        nextKey = e.get // hold on to key in strong ref
        if (nextKey == null) {
          entry = entry.nextEntry
        }
      }

      true
    }

    /** The common parts of next() across different types of iterators */
    protected def nextEntry: WeakEntry[K, V] = {
      if (nextKey == null && !hasNext) throw new NoSuchElementException

      lastReturned = entry
      entry = entry.nextEntry
      currKey = nextKey
      nextKey = null.asInstanceOf[K]
      lastReturned
    }
  }

  /*
   * We should implement this as a primitive operation over the underlying array, but it can
   * cause a behaviour change in edge cases where:
   * - Someone modifies a map during iteration
   * - The insertion point is close to the iteration point.
   *
   * The reason this happens is that the iterator prefetches the following element before
   * returning from next (to simplify the implementation of hasNext) while the natural
   * implementation of foreach does not.
   *
   * It should be mentioned that modifying a map during iteration leads to unpredictable
   * results with either implementation.
   */
  protected final def foreachEntry[C](f: WeakEntry[K, V] => C) { entriesIterator.foreach(f) }

  /**
   * Remove all entries from table
   */
  def clearTable {
    // clear out ref queue. We don't need to expunge entries
    // since table is getting cleared.
    while (queue.poll != null) {}

    var i = 0
    while (i < table.length) { table(i) = null; i += 1 }
    tableSize = 0

    valueToEntry.clear

    // Allocation of array may have caused GC, which may have caused
    // additional entries to go stale.  Removing these entries from the
    // reference queue will make them eligible for reclamation.
    while (queue.poll != null) {}
  }

  private def newThreshold(size: Int) =
    ((size.toLong * _loadFactor) / loadFactorDenum).toInt

  private def resize(newSize: Int) {
    val oldTable = getTable
    table = new Array(newSize)
    transfer(oldTable, table)

    /*
     * If ignoring null elements and processing ref queue caused massive
     * shrinkage, then restore old table.  This should be rare, but avoids
     * unbounded expansion of garbage-filled tables.
     */
    if (tableSize >= threshold / 2) {
      threshold = newThreshold(newSize)
    } else {
      expungeStaleEntries
      transfer(table, oldTable)
      table = oldTable
    }
  }

  /** Transfers all entries from src to dest tables */
  private def transfer(src: Array[WeakEntry[K, V]], dest: Array[WeakEntry[K, V]]) {
    var i = 0
    while (i < src.length) {
      var e = src(i)
      src(i) = null
      while (e != null) {
        val next = e.nextEntry
        val key = e.get
        if (key == null) {
          e.nextEntry = null // Help GC
          valueToEntry.remove(e.value)
          e.value = null.asInstanceOf[V] //  "   "
          tableSize -= 1
        } else {
          val h = index(e.hash)
          e.nextEntry = dest(h)
          dest(h) = e
        }
        e = next
      }
      i += 1
    }
  }

  protected def elemEquals(x1: Any, x2: Any): Boolean = {
    m.toString match {
      case "Byte" | "Short" | "Char" | "Int" | "Long" | "Float" | "Double" => x1 == x2
      case _ => x1.asInstanceOf[AnyRef] eq x2.asInstanceOf[AnyRef]
    }
  }

  protected def elemHashCode(x: Any) = if (x == null) 0 else hash(x)

  protected final def improve(hcode: Int) = {
    var h: Int = hcode + ~(hcode << 9)
    h = h ^ (h >>> 14)
    h = h + (h << 4)
    h ^ (h >>> 10)
  }

  protected final def index(hcode: Int) = improve(hcode) & (table.length - 1)

  /**
   * Returns the table after first expunging stale entries.
   */
  private def getTable(): Array[WeakEntry[K, V]] = {
    expungeStaleEntries
    table
  }

  /**
   * Expunges stale entries from the table.
   */
  protected def expungeStaleEntries() {
    var e: WeakEntry[K, V] = null
    while ({ e = queue.poll.asInstanceOf[WeakEntry[K, V]]; e != null }) {
      queue synchronized {
        val i = index(e.hash)

        var prev = table(i)
        var p = prev
        var break = false
        while (p != null && !break) {
          val next = p.nextEntry
          if (p eq e) {
            if (prev eq e)
              table(i) = next
            else
              prev.nextEntry = next
            // Must not null out e.next;
            // stale entries may be in use by a HashIterator
            tableSize -= 1
            valueToEntry.remove(e.value)
            e.value = null.asInstanceOf[V] // Help GC
            break = true
          } else {
            prev = p
            p = next
          }
        }
      }
    }
  }

}

private object WeakIdentityBiHashTable {

  /**
   * Returns a power of two >= `target`.
   */
  private def powerOfTwo(target: Int): Int = {
    /* See http://bits.stephan-brumme.com/roundUpToNextPowerOfTwo.html */
    var c = target - 1
    c |= c >>> 1
    c |= c >>> 2
    c |= c >>> 4
    c |= c >>> 8
    c |= c >>> 16
    c + 1
  }

  // --- hashcode. See scala.runtime.ScalaRunTime
  @inline def hash(x: Any): Int = {
    if (x.isInstanceOf[java.lang.Number]) BoxesRunTime.hashFromNumber(x.asInstanceOf[java.lang.Number])
    else System.identityHashCode(x)
  }
  @inline def hash(dv: Double): Int = {
    val iv = dv.toInt
    if (iv == dv) return iv

    val lv = dv.toLong
    if (lv == dv) return lv.hashCode
    else dv.hashCode
  }
  @inline def hash(fv: Float): Int = {
    val iv = fv.toInt
    if (iv == fv) return iv

    val lv = fv.toLong
    if (lv == fv) return lv.hashCode
    else fv.hashCode
  }
  @inline
  def hash(lv: Long): Int = {
    val iv = lv.toInt
    if (iv == lv) iv else (lv ^ (lv >>> 32)).toInt
  }
  @inline def hash(x: Int): Int = x
  @inline def hash(x: Short): Int = x.toInt
  @inline def hash(x: Byte): Int = x.toInt
  @inline def hash(x: Char): Int = x.toInt
  @inline def hash(x: Number): Int = runtime.BoxesRunTime.hashFromNumber(x)
  @inline def hash(x: java.lang.Long): Int = {
    val iv = x.intValue
    val lv = x.longValue
    if (iv == x.longValue) iv else (lv ^ (lv >>> 32)).toInt
  }
  // end of hashcode see scala.runtime.ScalaRunTime ---

  /**
   * Returns index for Object x.
   */
  @inline def hash(x: Any, length: Int): Int = {
    val h = hash(x)
    // Multiply by -127, and left-shift to use least bit as part of hash
    ((h << 1) - (h << 8)) & (length - 1)
  }

  /**
   * Circularly traverses table of tableSize len.
   */
  private def nextKeyIndex(i: Int, length: Int): Int = {
    if (i + 1 < length) i + 1 else 0
  }

}
