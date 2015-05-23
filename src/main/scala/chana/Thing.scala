package chana

import akka.actor.Actor
import chana.timeseries.TFreq
import chana.timeseries.TSer
import chana.timeseries.descriptor.Content
import chana.reactor.Publisher

/**
 *
 * @author Caoyuan Deng
 */
trait Thing extends Actor with Publisher {

  def identifier: String

  def name: String

  def description: String
  def description_=(description: String)

  def serOf(freq: TFreq): Option[TSer]

  /**
   * The content of each symbol should be got automatailly from PersistenceManager.restoreContent
   * and keep it there without being refered to another one, so, we only give getter without setter.
   */
  def content: Content

  /**
   * A helper method which can be overridden to get another ser provider from identifier
   */
  def thingOf(identifier: String): Option[Thing] = None
}

