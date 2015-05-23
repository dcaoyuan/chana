package chana.timeseries.datasource

import chana.timeseries.TFreq
import chana.timeseries.descriptor.Descriptor
import java.util.Calendar
import java.util.logging.Level
import java.util.logging.Logger
import scala.reflect.ClassTag

/**
 * Securities' data source request contract. It know how to find and invoke
 * server by call createBindInstance().
 *
 * We simplely inherit Descriptor, we may think the bindClass provides
 * service for descriptor.
 *
 * most fields' default value should be OK.
 *
 * @param [S] DataServer
 * @author Caoyuan Deng
 */
abstract class DataContract[S: ClassTag] extends Descriptor[S] {
  private val log = Logger.getLogger(this.getClass.getName)

  @transient var reqId = 0

  /** symbol in source */
  var srcSymbol: String = _

  var datePattern: Option[String] = None
  var urlString: String = ""
  var isRefreshable: Boolean = false
  var refreshInterval: Int = 5000 // ms

  var toTime = Calendar.getInstance.getTimeInMillis
  var fromTime = 0L
  var loadedTime = 0L

  def isFreqSupported(freq: TFreq): Boolean

  /**
   * All dataserver will be implemented as singleton
   * @param none args are needed.
   */
  override def createServiceInstance(args: Any*): Option[S] = {
    lookupServiceTemplate(m.runtimeClass.asInstanceOf[Class[S]], "DataServers")
  }

  override def toString: String = displayName

  override def clone: DataContract[S] = {
    try {
      super.clone.asInstanceOf[DataContract[S]]
    } catch {
      case ex: CloneNotSupportedException => log.log(Level.SEVERE, ex.getMessage, ex); null
    }
  }
}

