package chana.timeseries.descriptor

import chana.timeseries.TFreq
import java.util.logging.Level
import java.util.logging.Logger
import scala.collection.mutable.ArrayBuffer

/**
 *
 * @author Caoyuan Deng
 */
class Content(var identifier: String) extends Cloneable {
  private val log = Logger.getLogger(this.getClass.getName)

  /** use List to store descriptor, so they can be ordered by index */
  private var descriptorBuf = ArrayBuffer[Descriptor[_]]()

  def descriptors: List[Descriptor[_]] = descriptorBuf.toList

  def addDescriptor(descriptor: Descriptor[_]) {
    if (!descriptorBuf.contains(descriptor)) {
      descriptorBuf += descriptor
      descriptor.containerContent = this
    }
  }

  def removeDescriptor(descriptor: Descriptor[_]) {
    descriptorBuf.remove(descriptorBuf.indexOf(descriptor))
  }

  def removeDescriptor(idx: Int) {
    descriptorBuf.remove(idx)
  }

  def indexOf(descriptor: Descriptor[_]): Int = {
    descriptorBuf.indexOf(descriptor)
  }

  def lastIndexOf[T <: Descriptor[_]](clz: Class[T]): Int = {
    var lastOne = null.asInstanceOf[T]
    for (descriptor <- descriptorBuf if clz.isInstance(descriptor)) {
      lastOne = descriptor.asInstanceOf[T]
    }

    if (lastOne != null) descriptorBuf.indexOf(lastOne) else -1
  }

  def clearDescriptors[T <: Descriptor[_]](clz: Class[T]) {
    /**
     * try to avoid java.util.ConcurrentModificationException by add those to
     * toBeRemoved, then call descriptorList.removeAll(toBeRemoved)
     */
    var toBeRemoved = List[Int]()
    var i = 0
    for (descriptor <- descriptorBuf) {
      if (clz.isInstance(descriptor)) {
        toBeRemoved ::= i
      }
      i += 1
    }

    for (i <- toBeRemoved) {
      descriptorBuf.remove(i)
    }
  }

  /**
   *
   * @param clazz the Class being looking up
   * @return found collection of Descriptor instances.
   *         If found none, return an empty collection other than null
   */
  def lookupDescriptors[T <: Descriptor[_]](clz: Class[T]): Seq[T] = {
    for (descriptor <- descriptorBuf if clz.isInstance(descriptor)) yield descriptor.asInstanceOf[T]
  }

  /**
   * Lookup the descriptorList of clazz (Indicator/Drawing/Source etc) with the same time frequency
   */
  def lookupDescriptors[T <: Descriptor[_]](clz: Class[T], freq: TFreq): Seq[T] = {
    for (descriptor <- descriptorBuf if clz.isInstance(descriptor) && descriptor.freq == freq)
      yield descriptor.asInstanceOf[T]
  }

  def lookupDescriptor[T <: Descriptor[_]](clz: Class[T], serviceClassName: String, freq: TFreq): Option[T] = {
    lookupDescriptors(clz) find (_.idEquals(serviceClassName, freq))
  }

  def lookupActiveDescriptor[T <: Descriptor[_]](clz: Class[T]): Option[T] = {
    lookupDescriptors(clz) find (_.active)
  }

  def createDescriptor[T <: Descriptor[_]](clz: Class[T], serviceClassName: String, freq: TFreq): Option[T] = {
    try {
      val descriptor = clz.newInstance
      descriptor.set(serviceClassName, freq)
      addDescriptor(descriptor)

      Some(descriptor.asInstanceOf[T])
    } catch {
      case ex: IllegalAccessException =>
        log.log(Level.WARNING, ex.getMessage, ex); None
      case ex: InstantiationException => log.log(Level.WARNING, ex.getMessage, ex); None
    }
  }

  override def clone: Content = {
    try {
      val newone = super.clone.asInstanceOf[Content]
      newone.descriptorBuf = descriptorBuf map { x =>
        val y = x.clone
        y.containerContent = newone
        y
      }
      newone
    } catch {
      case ex: Throwable => log.log(Level.WARNING, ex.getMessage, ex); null
    }
  }
}

