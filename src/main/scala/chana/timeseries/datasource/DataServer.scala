package chana.timeseries.datasource

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Props
import chana.reactor.Publisher
import chana.timeseries.TVal
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.TimeZone
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.concurrent.duration._

/**
 * This class will load the quote datas from data source to its data storage: quotes.
 *
 * @param [V] data storege type
 * @author Caoyuan Deng
 */
abstract class DataServer[V: ClassTag] extends Ordered[DataServer[V]] with Actor with ActorLogging with Publisher {

  type C <: DataContract[_]

  private case class RequestData(contracts: Iterable[C])
  /**
   * @Note due to bug in PartialFunction, the inner final case class will cause isDefinedAt won't be compiled
   * https://issues.scala-lang.org/browse/SI-7151 -- only fixed for 2.11.x
   */
  /* final*/ case class DataLoaded(values: Array[V], contract: C)
  /* final*/ case class DataProcessed(contract: C)
  /* final*/ case class Heartbeat(interval: Int)

  protected val EmptyValues = Array[V]()
  protected val LONG_LONG_AGO: Long = Long.MinValue

  protected val subscribingMutex = new Object
  // --- Following maps should be created once here, since server may be singleton:
  //private val contractToStorage = new HashMap[C, ArrayList[V]] // use ArrayList instead of ArrayBuffer here, for toArray performance
  private val _refreshContracts = mutable.Set[C]()
  /** a quick seaching map */
  private val _refreshSymbolToContract = mutable.Map[String, C]()
  // --- Above maps should be created once here, since server may be singleton

  private var isRefreshable = false

  /**
   * @note Beware of a case in producer-consumer module:
   * During process DataLoaded, we may have accepted lots Heartbeat.
   * The producer is the one who implements requestData(...) and publishs DataLoaded,
   * after data collected. For example, who reads the data file and produces values;
   * The consumer is the one who accept DataLoaded and implements processData(...).
   * When producer collects data very quickly, (much) faster than consumer, the
   * values that are carried by DataLoaded will be blocked and the datum are stored
   * in actor's mailbox, i.e. the memory. In extreme cases, the memory will be exhausted
   * finally.
   *
   * You have to balance in this case, if the data that to be collected are very
   * important, that cannot be lost, you should increase the memory or store data
   * in persistence cache first. Otherwise, you have to try to sync the producer and
   * the consumer.
   */
  private var flowCount: Int = _ // flow control that tries to balance request and process
  // --- a proxy actor for HeartBeat event etc, which will detect the speed of
  // refreshing requests, if consumer can not catch up the producer, will drop
  // some requests.
  def receive = publisherBehavior orElse {
    case Heartbeat(interval) =>
      if (isRefreshable && flowCount < 5) {
        // refresh from loadedTime for subscribedContracts
        try {
          log.debug("Got Heartbeat message, going to request data, flowCount={}", flowCount)
          requestActor ! RequestData(subscribedContracts)
        } catch {
          case ex: Throwable => log.warning(ex.getMessage)
        }
      } else {
        flowCount -= 1 // give chance to requestData
      }
      flowCount = math.max(0, flowCount) // avoid too big gap
  }

  {
    import context.dispatcher
    val heartbeatInterval = context.system.settings.config.getInt("wandou.dataserver.heartbeat")
    context.system.scheduler.schedule(1.seconds, heartbeatInterval.seconds, self, Heartbeat(heartbeatInterval))
  }

  final class RequestActor extends Actor {
    def receive = {
      case RequestData(contracts) =>
        try {
          flowCount += 1
          log.debug("Got RequestData message, going to request data, flowCount={}", flowCount)
          requestData(contracts)
        } catch {
          case ex: Throwable => log.warning(ex.getMessage)
        }
    }
  }

  final class ProcessActor extends Actor {
    def receive = {
      // @Note 'contract' may be null, for instance: batch tickers loaded with multiple symbols.
      case DataLoaded(values, contract) =>
        val t0 = System.currentTimeMillis
        try {
          flowCount -= 1
          log.info("Got DataLoaded message, going to process data, flowCount={}", flowCount)
          val loadedTime = processData(values, contract)
          if (contract ne null) {
            log.info("Processed data for {}", contract.srcSymbol)
            contract.loadedTime = math.max(loadedTime, contract.loadedTime)
          }
        } catch {
          case ex: Throwable => log.warning(ex.getMessage)
        }

        publish(DataProcessed(contract))
        log.info("Processed data in {} ms", System.currentTimeMillis - t0)
    }
  }

  /**
   * We'll separate requestActor and processActor, so the request and process routines can be balanced a bit.
   * Otherwise, if the RequestData messages were sent batched, there will be no chance to fetch DataLoaded message
   * before RequestData
   */
  private val requestActor = context.actorOf(Props(classOf[RequestActor]))
  private val processActor = context.actorOf(Props(classOf[ProcessActor]))

  // --- public interfaces

  def loadData(contracts: Iterable[C]) {
    log.info("Fired RequestData message for {}", contracts.map(_.srcSymbol))
    // transit to async load reactor to put requests in queue (actor's mailbox)
    requestActor ! RequestData(contracts)
  }

  /**
   * Implement this method to request data from data source.
   * It should publish DataLoaded event to enable processData and fire chained events,
   * such as TSerEvent.Loaded etc.
   *
   * @note If contract.fromTime equals ANCIENT_TIME, you may need to process this condition.
   * @param contracts
   * @publish DataLoaded
   */
  protected def requestData(contracts: Iterable[C])

  /**
   * Publish loaded data to local reactor (including this DataServer instance),
   * or to remote message system (by overridding it).
   * @Note this DataServer will react to DataLoaded with processData automatically if it
   * received this event
   * @See reactions += {...}
   */
  protected def publishData(msg: Any) {
    processActor ! msg
  }

  /**
   * @param values the TVal values
   * @param contract could be null
   * @return loadedTime
   */
  protected def processData(values: Array[V], contract: C): Long

  def startRefresh { isRefreshable = true }
  def stopRefresh { isRefreshable = false }

  // ----- subscribe/unsubscribe is used for refresh only

  def subscribe(contract: C): Unit = subscribingMutex synchronized {
    _refreshContracts += contract
    _refreshSymbolToContract += contract.srcSymbol -> contract
  }

  def unsubscribe(contract: C): Unit = subscribingMutex synchronized {
    cancelRequest(contract)
    _refreshContracts -= contract
    _refreshSymbolToContract -= contract.srcSymbol
  }

  def subscribedContracts = _refreshContracts
  def subscribedSrcSymbols = _refreshSymbolToContract

  def isContractSubsrcribed(contract: C): Boolean = {
    _refreshContracts contains contract
  }

  def isSymbolSubscribed(srcSymbol: String): Boolean = {
    _refreshSymbolToContract contains srcSymbol
  }

  /**
   * @TODO
   * temporary method? As in some data feed, the symbol is not unique,
   * it may be same in different exchanges with different secType.
   */
  def contractOf(srcSymbol: String): Option[C] = {
    _refreshSymbolToContract.get(srcSymbol)
  }

  def displayName: String
  def defaultDatePattern: String
  def sourceTimeZone: TimeZone
  /**
   * @return serial number, valid only when >= 0
   */
  def serialNumber: Int

  /**
   * Convert source sn to source id in format of :
   * sn (0-63)       id (64 bits)
   * 0               ..,0000,0000
   * 1               ..,0000,0001
   * 2               ..,0000,0010
   * 3               ..,0000,0100
   * 4               ..,0000,1000
   * ...
   * @return source id
   */
  def id: Long = {
    val sn = serialNumber
    assert(sn >= 0 && sn < 63, "source serial number should be between 0 to 63!")

    if (sn == 0) 0 else 1 << (sn - 1)
  }

  // -- end of public interfaces

  /** @Note DateFormat is not thread safe, so we always return a new instance */
  protected def dateFormatOf(timeZone: TimeZone): DateFormat = {
    val pattern = defaultDatePattern
    val dateFormat = new SimpleDateFormat(pattern)
    dateFormat.setTimeZone(timeZone)
    dateFormat
  }

  protected def isAscending(values: Array[_ <: TVal]): Boolean = {
    val size = values.length
    if (size <= 1) {
      true
    } else {
      var i = -1
      while ({ i += 1; i < size - 1 }) {
        if (values(i).time < values(i + 1).time) {
          return true
        } else if (values(i).time > values(i + 1).time) {
          return false
        }
      }
      false
    }
  }

  protected def cancelRequest(contract: C) {}

  override def compare(another: DataServer[V]): Int = {
    if (this.displayName.equalsIgnoreCase(another.displayName)) {
      if (this.hashCode < another.hashCode) -1
      else if (this.hashCode == another.hashCode) 0
      else 1
    } else {
      this.displayName.compareTo(another.displayName)
    }
  }

  override def toString: String = displayName
}

