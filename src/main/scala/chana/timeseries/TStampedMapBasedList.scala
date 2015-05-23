package chana.timeseries

import chana.collection.AbstractArrayList
import scala.collection.generic._
import scala.collection.mutable
import scala.collection.mutable.BufferLike
import scala.collection.mutable.Builder
import scala.collection.mutable.IndexedSeqOptimized
import scala.reflect.ClassTag

/**
 * A package class that implements timestamped Map based List, used to store
 * sparse time series var.
 *
 * Below is a performance test result for various colletions:
 * http://www.artima.com/weblogs/viewpost.jsp?thread=122295
 * --------------------- ArrayList ---------------------
 *  size     add     get     set iteradd  insert  remove
 *    10     121     139     191     435    3952     446
 *   100      72     141     191     247    3934     296
 *  1000      98     141     194     839    2202     923
 * 10000     122     144     190    6880   14042    7333
 * --------------------- LinkedList ---------------------
 *  size     add     get     set iteradd  insert  remove
 *    10     182     164     198     658     366     262
 *   100     106     202     230     457     108     201
 *  1000     133    1289    1353     430     136     239
 * 10000     172   13648   13187     435     255     239
 * ----------------------- Vector -----------------------
 *  size     add     get     set iteradd  insert  remove
 *    10     129     145     187     290    3635     253
 *   100      72     144     190     263    3691     292
 *  1000      99     145     193     846    2162     927
 * 10000     108     145     186    6871   14730    7135
 * -------------------- Queue tests --------------------
 *  size    addFirst     addLast     rmFirst      rmLast
 *    10         199         163         251         253
 *   100          98          92         180         179
 *  1000          99          93         216         212
 * 10000         111         109         262         384
 * ------------- TreeSet -------------
 *  size       add  contains   iterate
 *    10       746       173        89
 *   100       501       264        68
 *  1000       714       410        69
 * 10000      1975       552        69
 * ------------- HashSet -------------
 *  size       add  contains   iterate
 *    10       308        91        94
 *   100       178        75        73
 *  1000       216       110        72
 * 10000       711       215       100
 * ---------- LinkedHashSet ----------
 *  size       add  contains   iterate
 *    10       350        65        83
 *   100       270        74        55
 *  1000       303       111        54
 * 10000      1615       256        58
 *
 * ---------- TreeMap ----------
 *  size     put     get iterate
 *    10     748     168     100
 *   100     506     264      76
 *  1000     771     450      78
 * 10000    2962     561      83
 * ---------- HashMap ----------
 *  size     put     get iterate
 *    10     281      76      93
 *   100     179      70      73
 *  1000     267     102      72
 * 10000    1305     265      97
 * ------- LinkedHashMap -------
 *  size     put     get iterate
 *    10     354     100      72
 *   100     273      89      50
 *  1000     385     222      56
 * 10000    2787     341      56
 * ------ IdentityHashMap ------
 *  size     put     get iterate
 *    10     290     144     101
 *   100     204     287     132
 *  1000     508     336      77
 * 10000     767     266      56
 * -------- WeakHashMap --------
 *  size     put     get iterate
 *    10     484     146     151
 *   100     292     126     117
 *  1000     411     136     152
 * 10000    2165     138     555
 * --------- Hashtable ---------
 *  size     put     get iterate
 *    10     264     113     113
 *   100     181     105      76
 *  1000     260     201      80
 * 10000    1245     134      77
 *
 * @author  Caoyuan Deng
 * @version 1.0, 11/22/2006
 * @since   1.0.4
 */
final class TStampedMapBasedList[A: ClassTag](timestamps: TStamps) extends AbstractArrayList[A](None)
    with GenericTraversableTemplate[A, TStampedMapBasedList]
    with BufferLike[A, TStampedMapBasedList[A]]
    with IndexedSeqOptimized[A, TStampedMapBasedList[A]]
    with Builder[A, TStampedMapBasedList[A]] {

  override protected def initialSize = 16
  override protected def maxCapacity = Int.MaxValue

  private val timeToElementData = new mutable.HashMap[Long, A]()

  override def size: Int = timestamps.size

  override def isEmpty: Boolean = timestamps.isEmpty

  override def contains[A1 >: A](elem: A1) = timeToElementData.valuesIterator.contains(elem)

  override def toArray[B >: A: ClassTag]: Array[B] = {
    val length = timestamps.size
    val array = new Array[B](length)
    copyToArray(array, 0)
    array
  }

  override def copyToArray[B >: A](xs: Array[B], start: Int) {
    val length = timestamps.size
    var i = 0
    while (i < length) {
      val time = timestamps(i)
      xs(i) = timeToElementData.getOrElse(time, Null.value).asInstanceOf[B]
      i += 1
    }
  }

  def put(time: Long, elem: A): Boolean = {
    if (elem == null) {
      /** null value does not need to be put in map, this will spare the memory usage */
      return true
    }

    val idx = timestamps.indexOfOccurredTime(time)
    if (idx >= 0) {
      timeToElementData.put(time, elem)
      true
    } else {
      assert(false, "Add timestamps first before add an element!")
      false
    }
  }

  def apply(time: Long): A = timeToElementData.getOrElse(time, Null.value)

  def update(time: Long, elem: A) {
    if (timestamps.contains(time)) {
      timeToElementData.put(time, elem)
    } else {
      assert(false, "Time out of bounds = " + time)
    }
  }

  @deprecated("+(elem:A) is not supported by this collection! use put(long time, E o)", "0")
  override def +(elem: A): this.type = {
    assert(false, "+(elem:A) is not supported by this collection! use put(long time, E o)")
    this
  }

  @deprecated("insertOne(n: Int, elems:A*) is not supported by this collection! use put(long time, E o)", "0")
  override def insertOne(n: Int, elem: A) {
    assert(false, "insertOne(n: Int, elems:A*) is not supported by this collection! use put(long time, E o)")
  }

  @deprecated("insertAll(n: Int, elems:Traversable[A]) is not supported by this collection! use put(long time, E o)", "0")
  override def insertAll(n: Int, elems: scala.collection.Traversable[A]) {
    assert(false, "insertAll(n: Int, elems:Traversable[A]) is not supported by this collection! use put(long time, E o)")
  }

  override def clear { timeToElementData.clear }

  override def equals(o: Any): Boolean = timeToElementData.equals(o)

  override def hashCode: Int = timeToElementData.hashCode

  override def apply(n: Int): A = {
    val time = timestamps(n)
    if (Null.not(time)) timeToElementData.get(time).get else null.asInstanceOf[A]
  }

  override def update(n: Int, newelem: A): Unit = {
    if (n >= 0 && n < timestamps.size) {
      val time = timestamps(n)
      timeToElementData.put(time, newelem)
    } else assert(false, "Index out of bounds! index = " + n)
  }

  override def remove(n: Int): A = {
    if (n >= 0 && n < timestamps.size) {
      val time = timestamps(n)
      val e = timeToElementData.get(n).get
      timeToElementData.remove(time)
      e
    } else {
      null.asInstanceOf[A]
    }
  }

  override def indexOf[B >: A](elem: B): Int = {
    val itr = timeToElementData.keysIterator
    while (itr.hasNext) {
      val time = itr.next
      if (timeToElementData.get(time).get == elem) {
        return binarySearch(time, 0, timestamps.size - 1)
      }
    }

    return -1
  }

  override def lastIndexOf[B >: A](elem: B): Int = {
    var found = -1
    val itr = timeToElementData.keysIterator
    while (itr.hasNext) {
      val time = itr.next
      if (timeToElementData.get(time).get == elem) {
        found = binarySearch(time, 0, timestamps.size - 1)
      }
    }

    found
  }

  private def binarySearch(time: Long, left: Int, right: Int): Int = {
    if (left == right) {
      if (timestamps(left) == time) left else -1
    } else {
      val middle = ((left + right) * 0.5).toInt
      if (time < timestamps(middle)) {
        if (middle == 0) -1 else binarySearch(time, left, middle - 1)
      } else {
        binarySearch(time, middle, right)
      }
    }
  }

  // --- methods inherited from traits

  @deprecated("Unsupported operation", "0")
  override def companion: GenericCompanion[TStampedMapBasedList] = throw new UnsupportedOperationException()

  def result: TStampedMapBasedList[A] = this

  override def reverse: TStampedMapBasedList[A] = {
    new TStampedMapBasedList[A](timestamps.reverse.asInstanceOf[TStamps])
  }

  @deprecated("Unsupported operation", "0")
  override def partition(p: A => Boolean): (TStampedMapBasedList[A], TStampedMapBasedList[A]) = {
    throw new UnsupportedOperationException
  }

  /**
   * Return a clone of this buffer.
   *
   *  @return an <code>ArrayList</code> with the same elements.
   */
  override def clone(): TStampedMapBasedList[A] = new TStampedMapBasedList[A](timestamps.clone)
}

