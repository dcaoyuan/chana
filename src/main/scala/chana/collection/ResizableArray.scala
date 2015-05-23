/*                     __                                               *\
 **     ________ ___   / /  ___     Scala API                            **
 **    / __/ __// _ | / /  / _ |    (c) 2003-2009, LAMP/EPFL             **
 **  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
 ** /____/\___/_/ |_/____/_/ | |                                         **
 **                          |/                                          **
 \*                                                                      */

// $Id: ResizableArray.scala 19219 2009-10-22 09:43:14Z moors $

package chana.collection

import scala.collection.generic._
import scala.collection.mutable.Builder
import scala.collection.mutable.IndexedSeq
import scala.collection.mutable.IndexedSeqOptimized
import scala.reflect.ClassTag

/**
 * This class is used internally to implement data structures that
 *  are based on resizable arrays.
 *
 *  @tparam A    type of the elements contained in this resizeable array.
 *
 *  @author  Matthias Zenger, Burak Emir
 *  @author Martin Odersky
 *  @author Caoyuan Deng
 *  @version 2.8
 *  @since   1
 */
trait ResizableArray[A] extends IndexedSeq[A]
    with GenericTraversableTemplate[A, ResizableArray]
    with IndexedSeqOptimized[A, ResizableArray[A]] {

  /** trait can not have type parameters like ResizableArray[A : ClassTag], so we have define an implicit val here */
  protected implicit val m: ClassTag[A]

  override def companion: GenericCompanion[ResizableArray] = ResizableArray

  protected def elementClass: Option[Class[A]] = None
  protected def initialSize: Int = 16
  protected def maxCapacity: Int = Int.MaxValue
  protected[collection] var cursor0: Int = 0

  protected[collection] var array: Array[A] = makeArray(initialSize)

  /**
   * Under case of calling new ArrayList[T] where T is not specified explicitly
   * during compile time, ie. the T is got during runtime, 'm' may be "_ <: Any",
   * or, AnyRef. In this case, we should pass in an elementClass and create array via
   *   java.lang.reflect.Array.newInstance(x, size)
   */
  protected def makeArray(size: Int) = {
    elementClass match {
      case Some(x) =>
        java.lang.reflect.Array.newInstance(x, size).asInstanceOf[Array[A]]
      case None =>
        // If the A is specified under compile time, that's ok, an Array[A] will be 
        // created (if A is primitive type, will also create a primitive typed array @see scala.reflect.Manifest)
        // If A is specified under runtime, will create a Array[AnyRef]
        new Array[A](size)
    }
  }

  protected var size0: Int = 0

  //##########################################################################
  // implement/override methods of IndexedSeq[A]

  /**
   * Returns the length of this resizable array.
   */
  def length: Int = size0

  /**
   * Translate idx to true idx of underlying array according to cursor0
   * if idx >= size0, will return -1
   */
  final protected def trueIdx(idx: Int) = {
    val idx1 = cursor0 + idx
    if (idx1 < size0) {
      idx1
    } else {
      idx1 - size0
    }
  }

  final protected def shiftCursor(n: Int) {
    cursor0 += n
    while (cursor0 >= maxCapacity) {
      cursor0 -= maxCapacity
    }
  }

  def apply(idx: Int) = {
    if (idx >= size0) throw new IndexOutOfBoundsException(idx.toString)
    array(trueIdx(idx))
  }

  def update(idx: Int, elem: A) {
    if (idx >= size0) throw new IndexOutOfBoundsException(idx.toString)
    array(trueIdx(idx)) = elem
  }

  override def foreach[U](f: A => U) {
    var i = 0
    while (i < size0) {
      f(array(trueIdx(i)))
      i += 1
    }
  }

  /**
   * Fills the given array <code>xs</code> with at most `len` elements of
   *  this traversable starting at position `start`.
   *  Copying will stop once either the end of the current traversable is reached or
   *  `len` elements have been copied or the end of the array is reached.
   *
   *  @param  xs the array to fill.
   *  @param  start starting index.
   *  @param  len number of elements to copy
   */
  override def copyToArray[B >: A](xs: Array[B], start: Int, len: Int) {
    val len1 = math.min(math.min(len, xs.length - start), size)
    if (len1 > 0) {
      val srcStart1 = trueIdx(0)
      val nBehindCursor = size0 - srcStart1
      if (nBehindCursor >= len1) {
        scala.compat.Platform.arraycopy(array, srcStart1, xs, start, len1)
      } else {
        scala.compat.Platform.arraycopy(array, srcStart1, xs, start, nBehindCursor)
        scala.compat.Platform.arraycopy(array, 0, xs, start + nBehindCursor, len1 - nBehindCursor)
      }
    }
  }

  //##########################################################################

  /**
   * remove elements of this array at indices after <code>sz</code>
   */
  def reduceToSize(sz: Int) {
    require(sz <= size0)
    if (cursor0 == 0) {
      if (m.runtimeClass.isPrimitive) {
        while (size0 > sz) {
          size0 -= 1
        }
      } else {
        while (size0 > sz) {
          size0 -= 1
          array(size0) = null.asInstanceOf[A]
        }
      }
    } else {
      val newArray = makeArray(array.length)

      val nBehindCursor = math.min(sz, size0 - cursor0)
      if (nBehindCursor > 0) {
        scala.compat.Platform.arraycopy(array, cursor0, newArray, 0, nBehindCursor)
      }
      val nBeforeCursor = sz - nBehindCursor
      if (nBeforeCursor > 0) {
        scala.compat.Platform.arraycopy(array, 0, newArray, nBehindCursor, nBeforeCursor)
      }

      array = newArray
      cursor0 = 0
      size0 = sz
    }
  }

  /** Ensure that the internal array has at least `n` cells. */
  protected def ensureSize(n: Int) {
    // Use a Long to prevent overflows
    val arrayLength: Long = array.length
    if (n > arrayLength) {
      var newSize: Long = arrayLength + initialSize
      while (newSize <= n)
        newSize += initialSize
      // Clamp newSize to maxCapacity 
      if (newSize > maxCapacity) newSize = maxCapacity

      val newArray = makeArray(newSize.toInt)
      scala.compat.Platform.arraycopy(array, 0, newArray, 0, size0)
      array = newArray
    }
  }

  /**
   * Swap two elements of this array.
   */
  protected def swap(a: Int, b: Int) {
    val a1 = trueIdx(a)
    val b1 = trueIdx(b)
    val h = array(a1)
    array(a1) = array(b1)
    array(b1) = h
  }
}

object ResizableArray extends SeqFactory[ResizableArray] {
  implicit def canBuildFrom[A]: CanBuildFrom[Coll, A, ResizableArray[A]] = new GenericCanBuildFrom[A]
  def newBuilder[A]: Builder[A, ResizableArray[A]] = new ArrayList[Any].asInstanceOf[ArrayList[A]]
}
