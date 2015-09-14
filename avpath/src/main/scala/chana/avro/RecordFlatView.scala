package chana.avro

import org.apache.avro.AvroRuntimeException
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import scala.collection.AbstractIterable
import scala.collection.AbstractIterator
import scala.collection.GenIterable
import scala.collection.IterableView
import scala.collection.generic.CanBuildFrom
import scala.collection.generic.GenericCompanion
import scala.collection.generic.TraversableFactory
import scala.collection.immutable
import scala.collection.mutable.Builder

final class RecordFlatView(underlying: GenericRecord, flattenField: String) extends AbstractIterable[GenericRecord] {
  lazy val fieldIterable = underlying.get(flattenField) match {
    case iterable: java.lang.Iterable[AnyRef] @unchecked => iterable
    case _ => throw new AvroRuntimeException("Not an iterable field: " + flattenField)
  }

  override def companion: GenericCompanion[Iterable] = RecordFlatView

  override def seq = this

  override def iterator: Iterator[GenericRecord] = new IteratorWrapper(underlying, flattenField, fieldIterable.iterator)

  override def foreach[U](f: GenericRecord => U): Unit = iterator.foreach(f)
  override def forall(p: GenericRecord => Boolean): Boolean = iterator.forall(p)
  override def exists(p: GenericRecord => Boolean): Boolean = iterator.exists(p)
  override def find(p: GenericRecord => Boolean): Option[GenericRecord] = iterator.find(p)
  override def isEmpty: Boolean = !iterator.hasNext
  override def foldRight[B](z: B)(op: (GenericRecord, B) => B): B = iterator.foldRight(z)(op)
  override def reduceRight[B >: GenericRecord](op: (GenericRecord, B) => B): B = iterator.reduceRight(op)
  override def toIterable: Iterable[GenericRecord] = thisCollection
  override def head: GenericRecord = iterator.next()

  override def slice(from: Int, until: Int): Iterable[GenericRecord] = {
    val lo = math.max(from, 0)
    val elems = until - lo
    val b = newBuilder
    if (elems <= 0) b.result()
    else {
      b.sizeHintBounded(elems, this)
      var i = 0
      val it = iterator drop lo
      while (i < elems && it.hasNext) {
        b += it.next
        i += 1
      }
      b.result()
    }
  }

  override def take(n: Int): Iterable[GenericRecord] = {
    val b = newBuilder

    if (n <= 0) b.result()
    else {
      b.sizeHintBounded(n, this)
      var i = 0
      val it = iterator
      while (i < n && it.hasNext) {
        b += it.next
        i += 1
      }
      b.result()
    }
  }

  override def drop(n: Int): Iterable[GenericRecord] = {
    val b = newBuilder
    val lo = math.max(0, n)
    b.sizeHint(this, -lo)
    var i = 0
    val it = iterator
    while (i < n && it.hasNext) {
      it.next()
      i += 1
    }
    (b ++= it).result()
  }

  override def takeWhile(p: GenericRecord => Boolean): Iterable[GenericRecord] = {
    val b = newBuilder
    val it = iterator
    while (it.hasNext) {
      val x = it.next()
      if (!p(x)) return b.result()
      b += x
    }
    b.result()
  }

  override def grouped(size: Int): Iterator[Iterable[GenericRecord]] =
    for (xs <- iterator grouped size) yield {
      val b = newBuilder
      b ++= xs
      b.result()
    }

  override def sliding(size: Int): Iterator[Iterable[GenericRecord]] = sliding(size, 1)

  override def sliding(size: Int, step: Int): Iterator[Iterable[GenericRecord]] =
    for (xs <- iterator.sliding(size, step)) yield {
      val b = newBuilder
      b ++= xs
      b.result()
    }

  override def takeRight(n: Int): Iterable[GenericRecord] = {
    val b = newBuilder
    b.sizeHintBounded(n, this)
    val lead = this.iterator drop n
    val it = this.iterator
    while (lead.hasNext) {
      lead.next()
      it.next()
    }
    while (it.hasNext) b += it.next()
    b.result()
  }

  override def dropRight(n: Int): Iterable[GenericRecord] = {
    val b = newBuilder
    if (n >= 0) b.sizeHint(this, -n)
    val lead = iterator drop n
    val it = iterator
    while (lead.hasNext) {
      b += it.next
      lead.next()
    }
    b.result()
  }

  override def copyToArray[B >: GenericRecord](xs: Array[B], start: Int, len: Int) {
    var i = start
    val end = (start + len) min xs.length
    val it = iterator
    while (i < end && it.hasNext) {
      xs(i) = it.next()
      i += 1
    }
  }

  override def zip[A1 >: GenericRecord, B, That](that: GenIterable[B])(implicit bf: CanBuildFrom[Iterable[GenericRecord], (A1, B), That]): That = {
    val b = bf(repr)
    val these = this.iterator
    val those = that.iterator
    while (these.hasNext && those.hasNext)
      b += ((these.next(), those.next()))
    b.result()
  }

  override def zipAll[B, A1 >: GenericRecord, That](that: GenIterable[B], thisElem: A1, thatElem: B)(implicit bf: CanBuildFrom[Iterable[GenericRecord], (A1, B), That]): That = {
    val b = bf(repr)
    val these = this.iterator
    val those = that.iterator
    while (these.hasNext && those.hasNext)
      b += ((these.next(), those.next()))
    while (these.hasNext)
      b += ((these.next(), thatElem))
    while (those.hasNext)
      b += ((thisElem, those.next()))
    b.result()
  }

  override def zipWithIndex[A1 >: GenericRecord, That](implicit bf: CanBuildFrom[Iterable[GenericRecord], (A1, Int), That]): That = {
    val b = bf(repr)
    var i = 0
    for (x <- this) {
      b += ((x, i))
      i += 1
    }
    b.result()
  }

  override def sameElements[B >: GenericRecord](that: GenIterable[B]): Boolean = {
    val these = this.iterator
    val those = that.iterator
    while (these.hasNext && those.hasNext)
      if (these.next != those.next)
        return false

    !these.hasNext && !those.hasNext
  }

  override def toStream: Stream[GenericRecord] = iterator.toStream

  override def canEqual(that: Any) = true

  override def view = new IterableView[GenericRecord, Iterable[GenericRecord]] {
    protected lazy val underlying = this.repr.asInstanceOf[Iterable[GenericRecord]]
    def iterator = RecordFlatView.this.iterator
  }

  override def view(from: Int, until: Int) = view.slice(from, until)

  /* The following methods are inherited from trait IterableLike
   *
   override def takeRight(n: Int): Iterable[A]
   override def dropRight(n: Int): Iterable[A]
   override def sameElements[B >: A](that: GenIterable[B]): Boolean
   override def view
   override def view(from: Int, until: Int)
   */

}

object RecordFlatView extends TraversableFactory[Iterable] {

  /** $genericCanBuildFromInfo */
  implicit def canBuildFrom[GenericRecord]: CanBuildFrom[Coll, GenericRecord, Iterable[GenericRecord]] = ReusableCBF.asInstanceOf[GenericCanBuildFrom[GenericRecord]]

  def newBuilder[GenericRecord]: Builder[GenericRecord, Iterable[GenericRecord]] = immutable.Iterable.newBuilder[GenericRecord]
}

final class IteratorWrapper(record: GenericRecord, flattenField: String, underlying: java.util.Iterator[AnyRef]) extends AbstractIterator[GenericRecord] with Iterator[GenericRecord] {
  def hasNext = underlying.hasNext
  def next() = new RecordWrapper(record, flattenField, underlying.next)
}

final class RecordWrapper(underlying: GenericRecord, flattenField: String, fieldValue: AnyRef) extends GenericRecord {
  def getSchema() = underlying.getSchema
  def put(key: String, value: AnyRef) {
    val field = getSchema.getField(key)
    if (field eq null) {
      throw new AvroRuntimeException("Not a valid schema field: " + key)
    } else {
      underlying.put(key, value)
    }

  }

  def put(i: Int, v: AnyRef) {
    underlying.put(i, v)
  }

  def get(key: String): AnyRef = {
    if (key == flattenField) {
      fieldValue
    } else {
      val field = getSchema.getField(key)
      if (field eq null) {
        null
      } else {
        underlying.get(key)
      }
    }
  }

  def get(i: Int): AnyRef = { underlying.get(i) }

  def compareTo(that: GenericRecord): Int = GenericData.get().compare(this, that, getSchema)
  override def equals(o: Any): Boolean = {
    o match {
      case that: GenericRecord =>
        if (that eq this) {
          true
        } else if (!this.getSchema.equals(that.getSchema)) {
          false
        } else {
          GenericData.get().compare(this, that, getSchema) == 0 // TODO ignore order = true
        }
      case _ => false
    }
  }
  override def hashCode(): Int = GenericData.get().hashCode(this, getSchema)
  override def toString(): String = GenericData.get().toString(this)
}
