/*                     __                                               *\
 **     ________ ___   / /  ___     Scala API                            **
 **    / __/ __// _ | / /  / _ |    (c) 2003-2010, LAMP/EPFL             **
 **  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
 ** /____/\___/_/ |_/____/_/ | |                                         **
 **                          |/                                          **
 \*                                                                      */

// $Id: ArrayList.scala 19223 2009-10-22 10:43:02Z malayeri $

package chana.collection

import scala.collection.CustomParallelizable
import scala.collection.generic._
import scala.collection.mutable.Buffer
import scala.collection.mutable.BufferLike
import scala.collection.mutable.Builder
import scala.collection.mutable.IndexedSeqOptimized
import scala.collection.mutable.Seq
import scala.collection.mutable.WrappedArray
import scala.collection.parallel.mutable.ParArray
import scala.reflect.ClassTag

/**
 * An implementation of the <code>Buffer</code> class using an array to
 *  represent the assembled sequence internally. Append, update and random
 *  access take constant time (amortized time). Prepends and removes are
 *  linear in the buffer size.
 *
 *  serialver -classpath ~/myapps/scala/lib/scala-library.jar:./ wandou.collection.ArrayList
 *
 *  @author  Matthias Zenger
 *  @author  Martin Odersky
 *  @version 2.8
 *  @since   1
 *
 *  @author  Caoyuan Deng
 *  @note Don't add @specialized in front of A, which will generate a shadow array and waste memory
 */

@SerialVersionUID(1529165946227428979L)
final class ArrayList[A](
  _initialSize: Int,
  _maxCapacity: Int,
  _elementClass: Option[Class[A]])(implicit _m: ClassTag[A]) extends AbstractArrayList[A](_elementClass)(_m)
    with GenericTraversableTemplate[A, ArrayList]
    with BufferLike[A, ArrayList[A]]
    with IndexedSeqOptimized[A, ArrayList[A]]
    with Builder[A, ArrayList[A]] {

  def this()(implicit m: ClassTag[A]) = this(16, Int.MaxValue, None)
  def this(initialSize: Int)(implicit m: ClassTag[A]) = this(initialSize, Int.MaxValue, None)
  def this(initialSize: Int, maxCapacity: Int)(implicit m: ClassTag[A]) = this(initialSize, maxCapacity, None)
  def this(maxCapacity: Int, elementClass: Option[Class[A]])(implicit m: ClassTag[A]) = this(16, maxCapacity, elementClass)
  def this(elementClass: Option[Class[A]])(implicit m: ClassTag[A]) = this(16, Int.MaxValue, elementClass)

  override def companion: GenericCompanion[ArrayList] = ArrayList

  override protected def initialSize = if (_maxCapacity < _initialSize) _maxCapacity else _initialSize
  override protected def maxCapacity = _maxCapacity

  def result: ArrayList[A] = this

  override def reverse: ArrayList[A] = {
    val reversed = new ArrayList[A](initialSize, maxCapacity, elementClass)
    var i = 0
    while (i < size) {
      reversed += apply(size - 1 - i)
      i += 1
    }
    reversed
  }

  override def partition(p: A => Boolean): (ArrayList[A], ArrayList[A]) = {
    val l, r = new ArrayList[A](initialSize, maxCapacity, elementClass)
    for (x <- this) (if (p(x)) l else r) += x
    (l, r)
  }

  /**
   * Return a clone of this buffer.
   *
   *  @return an <code>ArrayList</code> with the same elements.
   */
  override def clone(): ArrayList[A] = new ArrayList[A](initialSize, maxCapacity, elementClass) ++= this

  def sliceToArrayList(start: Int, len: Int): ArrayList[A] = {
    val res = new ArrayList(initialSize, maxCapacity, elementClass)
    res.array = this.sliceToArray(start, len)
    res
  }
}

/**
 * Factory object for <code>ArrayBuffer</code> class.
 *
 *  @author  Martin Odersky
 *  @version 2.8
 *  @since   2.8
 */
object ArrayList extends SeqFactory[ArrayList] {
  implicit def canBuildFrom[A]: CanBuildFrom[Coll, A, ArrayList[A]] = new GenericCanBuildFrom[A]
  /**
   * we implement newBuilder for extending SeqFactory only. Since it's with no ClassTag arg,
   * we can only create a ArrayList[Any] instead of ArrayList[A], but we'll define another
   * apply method to create ArrayList[A]
   */
  def newBuilder[A]: Builder[A, ArrayList[A]] = new ArrayList[Any]().asInstanceOf[ArrayList[A]]
  def apply[A: ClassTag]() = new ArrayList[A]
}

/** Explicit instantiation of the `Buffer` trait to reduce class file size in subclasses. */
private[collection] abstract class collectionAbstractTraversable[+A] extends scala.collection.Traversable[A]
private[collection] abstract class collectionAbstractIterable[+A] extends collectionAbstractTraversable[A] with scala.collection.Iterable[A]
private[collection] abstract class collectionAbstractSeq[+A] extends collectionAbstractIterable[A] with scala.collection.Seq[A]
private[collection] abstract class AbstractSeq[A] extends collectionAbstractSeq[A] with Seq[A]
private[collection] abstract class AbstractBuffer[A] extends AbstractSeq[A] with Buffer[A]

abstract class AbstractArrayList[A](_elementClass: Option[Class[A]])(implicit val m: ClassTag[A]) extends AbstractBuffer[A]
    with Buffer[A]
    with GenericTraversableTemplate[A, AbstractArrayList]
    with BufferLike[A, AbstractArrayList[A]]
    with IndexedSeqOptimized[A, AbstractArrayList[A]]
    with Builder[A, AbstractArrayList[A]]
    with ResizableArray[A]
    with CustomParallelizable[A, ParArray[A]]
    with Serializable {

  override def companion: GenericCompanion[AbstractArrayList] = AbstractArrayList

  override protected def elementClass: Option[Class[A]] = _elementClass

  def clear() {
    reduceToSize(0)
  }

  override def sizeHint(len: Int) {
    if (len > size && len >= 1) {
      val newarray = makeArray(len)
      scala.compat.Platform.arraycopy(array, 0, newarray, 0, size0)
      array = newarray
    }
  }

  override def par = ParArray.handoff[A](toArray, size)

  /**
   * Appends a single element to this buffer and returns
   *  the identity of the buffer. It takes constant time.
   *
   *  @param elem  the element to append.
   */
  def +(elem: A): this.type = {
    ensureSize(size0 + 1)

    if (size0 + 1 <= maxCapacity) {
      array(size0) = elem
      size0 += 1
    } else {
      shiftCursor(1)
      if (cursor0 == 0) {
        array(maxCapacity - 1) = elem
      } else {
        array(cursor0 - 1) = elem
      }
      size0 = maxCapacity
    }

    this
  }

  /**
   * Appends a single element to this buffer and returns
   *  the identity of the buffer. It takes constant time.
   *
   *  @param elem  the element to append.
   */
  def +=(elem: A): this.type = {
    this.+(elem)
  }

  /**
   * Appends a number of elements provided by an iterable object
   *  via its <code>iterator</code> method. The identity of the
   *  buffer is returned.
   *
   *  @param xs  the itertable object.
   */
  def ++(xs: TraversableOnce[A]): this.type = {
    val elems = xs match {
      // according to the Scala implementation in 2.8: An implicit conversion 
      // takes Java arrays to `WrappedArray` if you need a `Traversable` instance.  
      // @see https://lampsvn.epfl.ch/trac/scala/ticket/2564
      case xs: WrappedArray[A] => xs.array
      case _                   => xs.toArray
    }
    val len = elems.length
    ensureSize(size0 + len)

    if (size0 + len <= maxCapacity) {
      scala.compat.Platform.arraycopy(elems, 0, array, size0, len)
      size0 += len
    } else {
      val nOverride = len - (maxCapacity - size0)
      // move cursor to final position first
      shiftCursor(nOverride)
      if (cursor0 > 0) {
        val nFillBeforeCursor = math.min(len, cursor0)
        scala.compat.Platform.arraycopy(elems, len - nFillBeforeCursor, array, 0, nFillBeforeCursor)
        val nLeft = len - nFillBeforeCursor
        if (nLeft > 0) {
          val nFillBehindCursor = if (nLeft > maxCapacity - cursor0) maxCapacity - cursor0 else nLeft
          scala.compat.Platform.arraycopy(elems, len - nFillBeforeCursor - nFillBehindCursor, array, maxCapacity - nFillBehindCursor, nFillBehindCursor)
        }
      } else {
        val nFillAfterCursor = if (len > maxCapacity) maxCapacity else len
        scala.compat.Platform.arraycopy(elems, len - nFillAfterCursor, array, maxCapacity - nFillAfterCursor, nFillAfterCursor)
      }
      size0 = maxCapacity
    }

    this
  }

  /**
   * Appends a number of elements provided by an iterable object
   *  via its <code>iterator</code> method. The identity of the
   *  buffer is returned.
   *
   *  @param xs  the itertable object.
   *  @return    the updated buffer.
   */
  override def ++=(xs: TraversableOnce[A]): this.type = {
    this.++(xs)
  }

  /**
   * Prepends a single element to this buffer and return
   *  the identity of the buffer. It takes time linear in
   *  the buffer size.
   *
   *  @param elem  the element to append.
   */
  def +:(elem: A): this.type = {
    insertOne(0, elem)
    this
  }

  /**
   * Prepends a single element to this buffer and return
   *  the identity of the buffer. It takes time linear in
   *  the buffer size.
   *
   *  @param elem  the element to append.
   *  @return      the updated buffer.
   */
  def +=:(elem: A): this.type = {
    this.+:(elem)
  }

  /**
   * Prepends a number of elements provided by an iterable object
   *  via its <code>iterator</code> method. The identity of the
   *  buffer is returned.
   *
   *  @param xs  the iterable object.
   */
  def ++:(xs: TraversableOnce[A]): this.type = {
    insertAll(0, xs.toTraversable)
    this
  }

  /**
   * Prepends a number of elements provided by an iterable object
   *  via its <code>iterator</code> method. The identity of the
   *  buffer is returned.
   *
   *  @param xs  the iterable object.
   *  @return    the updated buffer.
   */
  override def ++=:(xs: TraversableOnce[A]): this.type = {
    this.++:(xs)
  }

  /**
   * Inserts new elements at a given index into this buffer.
   *
   *  @param n      the index where new elements are inserted.
   *  @param elems  the traversable collection containing the elements to insert.
   *  @throws   IndexOutOfBoundsException if the index `n` is not in the valid range
   *            `0 <= n <= length`.
   *
   *  override scala.collection.mutable.BufferLike.insert
   *  https://issues.scala-lang.org/browse/SI-7268
   */
  @deprecated("Use insertAll(n: Int, elems: Traversable[A]) or insertOne(n: Int, elem: A), this method may cause ArrayStoreException.", "Scala 2.10.0")
  override def insert(n: Int, elems: A*) {
    throw new UnsupportedOperationException("Use insertAll(n: Int, elems: Traversable[A]) or insertOne(n: Int, elem: A), this method may cause ArrayStoreException.")
  }

  def insertOne(n: Int, elem: A) {
    if ((n < 0) || (n > size0)) throw new IndexOutOfBoundsException(n.toString)
    if (n == size0) { // insert at position 'size', it behaves like appending at the end
      this.+(elem)
    } else {
      ensureSize(size0 + 1)
      if (size0 + 1 <= maxCapacity) {
        scala.compat.Platform.arraycopy(array, n, array, n + 1, size0 - n)
        array(n) = elem
        size0 += 1
      } else {
        val n1 = trueIdx(n)
        if (n1 == cursor0) {
          // do thing, we'll just drop this inserting elem
        } else if (n1 > cursor0) {
          // n1 is behind cursor, do not need to forward shift elems before cursor 
          // forward shift elems between cursor and n1 
          scala.compat.Platform.arraycopy(array, cursor0 + 1, array, cursor0, n1 - cursor0)
          array(n1) = elem
        } else { // n1 < cursor, n1 is before cursor 
          if (cursor0 != size0 - 1) {
            // 1. forward shift all elems after cursor 
            scala.compat.Platform.arraycopy(array, cursor0 + 1, array, cursor0, size0 - cursor0 - 1)
            // 2. move first to last
            array(size0 - 1) = array(0)
          } // else there is no elems after cursor
          // 3. forward shift elems between 0 and n1
          scala.compat.Platform.arraycopy(array, 0, array, 1, n1 - 1)
          array(n1) = elem
        }
        size0 = maxCapacity
      }
    }
  }

  /**
   * Inserts new elements at the index <code>n</code>. Opposed to method
   *  <code>update</code>, this method will not replace an element with a
   *  one. Instead, it will insert a new element at index <code>n</code>.
   *
   *  @param n     the index where a new element will be inserted.
   *  @param iter  the iterable object providing all elements to insert.
   *  @throws Predef.IndexOutOfBoundsException if <code>n</code> is out of bounds.
   */
  def insertAll(n: Int, elems: scala.collection.Traversable[A]) {
    if ((n < 0) || (n > size0)) throw new IndexOutOfBoundsException(n.toString)
    if (n == size0) { // insert at position 'size', it behaves like appending at the end
      this.++(elems)
    } else {
      val xs = elems match {
        // according to the way arrays work in 2.8: An implicit conversion takes 
        // Java arrays to `WrappedArray` if you need a `Traversable` instance.  
        // @see https://lampsvn.epfl.ch/trac/scala/ticket/2564
        case elems: WrappedArray[A] => elems.array
        case _                      => elems.toArray
      }
      val len = xs.length
      ensureSize(size0 + len)
      if (size0 + len <= maxCapacity) {
        scala.compat.Platform.arraycopy(array, n, array, n + len, size0 - n)
        scala.compat.Platform.arraycopy(xs.array, 0, array, n, len)
        size0 += len
      } else {
        val n1 = trueIdx(n)

        val nVacancy = maxCapacity - size0
        val nForwardShiftable = if (n1 >= cursor0) n1 - cursor0 else n1 + (size0 - cursor0)
        val nInsertable = math.min(nVacancy + nForwardShiftable, len)
        if (nInsertable > 0) {
          if (nVacancy > 0) {
            // and cursor0 should be 0 too, since we'll always keep cursor0 is at 0 in this case  
            assert(cursor0 == 0, "When there are vacancy still available, cursor0 should keep to 0")
            // backward shift all elems after n1 by nVacancy steps 
            scala.compat.Platform.arraycopy(array, n1, array, n1 + nVacancy, size0 - n1)
            // fill 'nInsertable' elems at 'n1 + nVacancy - nInsertable'
            scala.compat.Platform.arraycopy(xs, len - nInsertable, array, n1 + nVacancy - nInsertable, nInsertable)
          } else {
            if (n1 == cursor0) {
              // do thing, we'll just drop this inserting elem
            } else if (n1 > cursor0) {
              // n1 is behind cursor, do not need to forward shift elems before cursor 
              // forward shift elems between cursor and n1 
              val nFilled = math.min(len, n1 - cursor0)
              scala.compat.Platform.arraycopy(array, cursor0 + nFilled, array, cursor0, nFilled)
              scala.compat.Platform.arraycopy(xs, len - nFilled, array, n1 - nFilled, nFilled)
            } else { // n1 < cursor, n1 is before cursor 
              if (cursor0 != size0 - 1) {
                // 1. forward shift all elems after cursor 
                scala.compat.Platform.arraycopy(array, cursor0 + 1, array, cursor0, size0 - cursor0 - 1)
                // 2. move first to last
                array(size0 - 1) = array(0)
              } // else there is no elem after cursor  
              // 3. forward shift elems between 0 and n1
              scala.compat.Platform.arraycopy(array, 0, array, 1, n1 - 1)
            }
          }
        } // else nInsertable is 0, do thing, we'll just drop all inserting elems

        size0 = maxCapacity
      }
    }
  }

  /**
   * Removes the element on a given index position. It takes time linear in
   *  the buffer size.
   *
   *  @param n  the index which refers to the first element to delete.
   *  @param count   the number of elemenets to delete
   *  @throws Predef.IndexOutOfBoundsException if <code>n</code> is out of bounds.
   */
  override def remove(n: Int, count: Int) {
    require(count >= 0, "removing negative number of elements")
    if (n < 0 || n > size0 - count) throw new IndexOutOfBoundsException(n.toString)
    if (cursor0 == 0) {
      scala.compat.Platform.arraycopy(array, n + count, array, n, size0 - (n + count))
    } else {
      // we'll remove count elems and get cursor0 to 0 again 
      val n1 = trueIdx(n)
      if (n1 < cursor0) {
        scala.compat.Platform.arraycopy(array, n1 + count, array, n1, cursor0 - (n1 + count))
      } else {
        val nBehindCursor = math.min(count, size0 - n1)
        // move elems behind cursor first
        scala.compat.Platform.arraycopy(array, n1 + nBehindCursor, array, n1, size0 - (n1 + nBehindCursor))
        val nBeforeCursor = math.max(0, count - nBehindCursor)
        if (nBehindCursor > 0) {
          // there is nBehindCursor cells left behind cursor, should fill them with elems before cursor 
          val nMoveToBehind = math.min(nBehindCursor, cursor0 - nBeforeCursor)
          if (nMoveToBehind > 0) {
            // copy to behind
            scala.compat.Platform.arraycopy(array, nBeforeCursor, array, size0 - nBehindCursor, nMoveToBehind)
            // before cursor, shift left elems 
            val stepShift = nBeforeCursor + nMoveToBehind
            scala.compat.Platform.arraycopy(array, stepShift, array, 0, cursor0 - stepShift)
          }
        } else {
          if (nBeforeCursor > 0) {
            scala.compat.Platform.arraycopy(array, nBeforeCursor, array, 0, cursor0 - nBeforeCursor)
          }
        }
      }
    }

    reduceToSize(size0 - count)
  }

  /**
   * Removes the element on a given index position
   *
   *  @param n  the index which refers to the element to delete.
   *  @return  The element that was formerly at position `n`
   */
  def remove(n: Int): A = {
    val result = apply(trueIdx(n))
    remove(n, 1)
    result
  }

  /**
   * Defines the prefix of the string representation.
   */
  override def stringPrefix: String = "ArrayList"

  /**
   * We need this toArray to export an array with the original type element instead of
   * scala.collection.TraversableOnce#toArray[B >: A : ClassTag]: Array[B]:
   * def toArray[B >: A : ClassTag]: Array[B] = {
   *   if (isTraversableAgain) {
   *     val result = new Array[B](size)
   *     copyToArray(result, 0)
   *     result
   *   }
   *   else toBuffer.toArray
   * }
   */
  def toArray: Array[A] = {
    val res = makeArray(size0)

    val nBehindCursor = size0 - cursor0
    scala.compat.Platform.arraycopy(array, cursor0, res, 0, nBehindCursor)
    val nBeforeCursor = size0 - nBehindCursor
    if (nBeforeCursor > 0) {
      scala.compat.Platform.arraycopy(array, 0, res, nBehindCursor, nBeforeCursor)
    }

    res
  }

  /**
   * fill 'len' elememts from 'start' to array. fill (this.length - start) if
   * 'len' is less than (this.length - start).
   *
   * @param start   start index to be copied
   * @param len     len of elements to be copied
   */
  def sliceToArray(start: Int, len: Int): Array[A] = {
    val len1 = math.min(len, size0 - start)
    if (len1 > 0) {
      val xs = makeArray(len)
      val srcStart1 = trueIdx(start)
      val nBehindCursor = size0 - srcStart1
      if (nBehindCursor >= len1) {
        scala.compat.Platform.arraycopy(array, srcStart1, xs, 0, len1)
      } else {
        scala.compat.Platform.arraycopy(array, srcStart1, xs, 0, nBehindCursor)
        scala.compat.Platform.arraycopy(array, 0, xs, nBehindCursor, len1 - nBehindCursor)
      }

      xs
    } else {
      Array()
    }
  }

  // --- overrided methods for performance

  override def head: A = {
    if (isEmpty) throw new NoSuchElementException
    else apply(0)
  }

  override def last: A = {
    if (isEmpty) throw new NoSuchElementException
    else apply(size0 - 1)
  }

}

object AbstractArrayList extends SeqFactory[AbstractArrayList] {
  implicit def canBuildFrom[A]: CanBuildFrom[Coll, A, AbstractArrayList[A]] = new GenericCanBuildFrom[A]
  /**
   * we implement newBuilder for extending SeqFactory only. Since it's with no ClassTag arg,
   * we can only create a ArrayList[Any] instead of ArrayList[A], but we'll define another
   * apply method to create ArrayList[A]
   */
  def newBuilder[A]: Builder[A, AbstractArrayList[A]] = new ArrayList[Any]().asInstanceOf[ArrayList[A]]
}

