package chana.timeseries.descriptor

import chana.timeseries.TFreq
import chana.timeseries.datasource.PersistenceManager
import java.util.logging.Level
import java.util.logging.Logger
import scala.reflect.ClassTag

/**
 * Descriptor is something like NetBeans' DataObject
 *
 * [S] Service class type
 *
 * @author Caoyuan Deng
 */
abstract class Descriptor[S](
    private var _serviceClassName: String,
    private var _freq: TFreq,
    private var _active: Boolean)(protected implicit val m: ClassTag[S]) extends Cloneable {

  private val log = Logger.getLogger(this.getClass.getName)

  /**
   * @note According to http://bits.netbeans.org/dev/javadoc/org-openide-modules/org/openide/modules/doc-files/classpath.html:
   * The basic thing you need to understand about how modules control class loading is this:
   *   If module B has a declared dependency on module A, then classes in B can refer to classes in A
   *   (but A cannot refer to B). If B does not have a declared dependency on A, it cannot refer to A.
   *   Furthermore, dependencies are not considered transitive for purposes of classloading: if C has
   *   a declared dependency on B, it can refer to classes in B, but not to A (unless it also declares
   *   an explicit dependency on A).
   *
   * Also @see http://wiki.netbeans.org/DevFaqModuleCCE
   * Also @see http://netbeans-org.1045718.n5.nabble.com/Class-forName-otherModuleClass-in-library-modules-td3021534.html
   */
  private val classLoader = Thread.currentThread.getContextClassLoader

  var containerContent: Content = _

  /** @Note: covariant type S can not occur in contravariant position in type S of parameter of setter */
  private var _serviceInstance: Option[_] = None

  def this()(implicit m: ClassTag[S]) {
    this(null, TFreq.DAILY, false)
  }

  def set(serviceClassName: String, freq: TFreq) {
    this.serviceClassName = serviceClassName
    this.freq = freq.clone
  }

  def serviceClassName = _serviceClassName
  def serviceClassName_=(serviceClassName: String) = {
    this._serviceClassName = serviceClassName
  }

  def freq = _freq
  def freq_=(freq: TFreq) = {
    this._freq = freq
  }

  def active = _active
  def active_=(active: Boolean) = {
    this._active = active
  }

  def displayName: String

  def resetInstance {
    _serviceInstance = None
  }

  def idEquals(serviceClassName: String, freq: TFreq): Boolean = {
    this.serviceClassName.equals(serviceClassName) && this.freq.equals(freq)
  }

  /**
   * init and return a server instance
   * @param args args to init server instance
   */
  def createdServerInstance: S = {
    assert(isServiceInstanceCreated, "This method should only be called after serviceInstance created!")
    serviceInstance().get
  }

  def serviceInstance(args: Any*): Option[S] = {
    if (_serviceInstance.isEmpty) {
      // @Note to pass a variable args to another function, should use type "_*" to extract it as a plain seq,
      // other wise, it will be treated as one arg:Seq[_], and the accepting function will compose it as
      // Seq(Seq(arg1, arg2, ...)) instead of Seq(arg1, arg2, ...)
      _serviceInstance = createServiceInstance(args: _*)
    }
    _serviceInstance.asInstanceOf[Option[S]]
  }

  def isServiceInstanceCreated: Boolean = {
    _serviceInstance.isDefined
  }

  protected def createServiceInstance(args: Any*): Option[S]

  // --- helpers ---

  protected def lookupServiceTemplate(tpe: Class[S], folderName: String): Option[S] = {
    val services = PersistenceManager().lookupAllRegisteredServices(tpe, folderName)
    services find { service =>
      val className = service.asInstanceOf[AnyRef].getClass.getName
      className == serviceClassName || className == (serviceClassName + "$") || (className + "$") == serviceClassName
    } match {
      case None =>
        try {
          log.warning("Cannot find registeredService of " + tpe + " in folder '" +
            folderName + "': " + services.map(_.asInstanceOf[AnyRef].getClass.getName) +
            ", try Class.forName call: serviceClassName=" + serviceClassName)

          val klass = Class.forName(serviceClassName, true, classLoader)

          getScalaSingletonInstance(klass) match {
            case Some(x: S) => Option(x)
            case _          => Option(klass.newInstance.asInstanceOf[S])
          }
        } catch {
          case ex: Exception =>
            log.log(Level.SEVERE, "Failed to call Class.forName of class: " + serviceClassName, ex)
            None
        }
      case some => some
    }
  }

  protected def isScalaSingletonClass(klass: Class[_]) = {
    klass.getSimpleName.endsWith("$") && klass.getInterfaces.exists(_.getName == "scala.ScalaObject") &&
      klass.getDeclaredFields.exists(_.getName == "MODULE$")
  }

  protected def getScalaSingletonInstance(klass: Class[_]): Option[AnyRef] = {
    if (klass.getSimpleName.endsWith("$") && klass.getInterfaces.exists(_.getName == "scala.ScalaObject")) {
      klass.getDeclaredFields.find(_.getName == "MODULE$") match {
        case Some(x) => Option(x.get(klass))
        case None    => None
      }
    } else None
  }

  override def clone: Descriptor[S] = {
    try {
      super.clone.asInstanceOf[Descriptor[S]]
    } catch {
      case ex: CloneNotSupportedException => log.log(Level.SEVERE, ex.getMessage, ex); null
    }
  }
}
