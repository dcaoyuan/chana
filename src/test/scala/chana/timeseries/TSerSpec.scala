package chana.timeseries

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.Props
import akka.pattern.ask
import akka.event.Logging
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import chana.Thing
import chana.timeseries.descriptor.Content
import com.typesafe.config.ConfigFactory
import java.util.Calendar
import java.util.TimeZone
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import scala.concurrent.duration._

class TSerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("TSerSpec", ConfigFactory.parseString("""
  akka.actor {
      provider = "akka.cluster.ClusterActorRefProvider"                                                                      
  }
  akka.persistence.journal.plugin = "akka.persistence.journal.leveldb"
  akka.remote.netty.tcp.hostname = "localhost"
  # To avoid:
  #   Logger specified in config can't be loaded [akka.event.slf4j.Slf4jLogger] 
  #   due to [akka.event.Logging$LoggerInitializationException: Logger log1-Slf4jLogger 
  #   did not respond with LoggerInitialized, sent instead [TIMEOUT]]                                                                    
  akka.logger-startup-timeout = 30s 
  # set port to random to by pass the ports that will be occupied by ChanaClusterSpec test
  akka.remote.netty.tcp.port = 0
  """)))

  val log = Logging(system, this.getClass)

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  def randomValue(min: Int, max: Int): Double = {
    min + (math.random * ((max - min) + 1))
  }

  val cal = Calendar.getInstance
  cal.set(2000, 0, 1, 23, 58, 0)
  val dataFreq = TFreq(TUnit.Second, 10)
  var time = dataFreq.round(cal.getTimeInMillis, cal)

  def snapshots = (1 to 100) map { i =>
    time = dataFreq.nextTime(time)
    val price = randomValue(1, 10)
    val volume = randomValue(1000, 5000)
    ExampleThing.Snapshot(time, price, volume)
  }

  "A thing" must {
    val thing = system.actorOf(Props(new ExampleThing))
    import system.dispatcher

    "insert data" in {
      snapshots foreach (thing ! _)
      expectSerSize(TFreq.ONE_MIN)(6, 17)
      expectSerSize(TFreq.DAILY)(6, 2)
      expectSerSize(TFreq.FIVE_MINS)(6, 4)

      // more snapshots to test ser merger
      snapshots foreach (thing ! _)
      expectSerSize(TFreq.ONE_MIN)(6, 34)
      expectSerSize(TFreq.DAILY)(6, 2)
      expectSerSize(TFreq.FIVE_MINS)(6, 8)

      def expectSerSize(freq: TFreq)(nVars: Int, nPeriods: Int) {
        thing ! TSer.Export(self, freq, 0, Long.MaxValue)
        expectMsgPF(5.seconds) {
          case x: scala.collection.Map[_, _] =>
            val ser = x.asInstanceOf[scala.collection.Map[String, Array[_]]]
            assert(ser.size == nVars)
            log.info("======== {}", freq)
            ser.foreach {
              case (k, v) =>
                log.info("{} : {}", k, v.mkString(","))
                assert(v.size == nPeriods)
            }
          case x =>
            log.error("Got {}", x)
            assert(false)
        }
      }
    }

  }
}

object ExampleThing {
  /**
   * price is current value
   * volume is the incremental number that is counted after previous snapshot
   */
  final case class Snapshot(time: Long, price: Double, volume: Double)
}

class ExampleThing() extends Actor with ActorLogging with Thing {

  import context.dispatcher

  def receive = {
    case x: ExampleThing.Snapshot =>
      sersOf(evtDrivenFreqs) foreach (_ ! x)
    case x: TSer.Export if supportedFreqs.contains(x.freq) =>
      serOf(x.freq) foreach (_ forward x)
    case x =>
      log.warning("ExampleThing received: {}", x)
  }

  var evtDrivenFreqs: Set[TFreq] = Set(TFreq.ONE_MIN, TFreq.DAILY)
  var supportedFreqs: Set[TFreq] = Set(TFreq.ONE_MIN, TFreq.DAILY, TFreq.FIVE_MINS)
  def sersOf(freqs: Set[TFreq]) = (freqs map serOf).flatten

  var thingSers = List[TSer]()
  def serOf(freq: TFreq): Option[TSer] = {
    thingSers.find(_.freq == freq) match {
      case None =>
        if (evtDrivenFreqs.contains(freq)) {
          val ser = new ExampleSer(this, freq)
          thingSers ::= ser
          Some(ser)
        } else if (supportedFreqs.contains(freq)) {
          val ser = new ExampleSer(this, freq)
          thingSers ::= ser
          mergedSer(ser)
          evtDrivenFreqs += freq
          Some(ser)
        } else {
          None
        }
      case some => some
    }
  }

  def identifier: String = ""
  def name: String = identifier
  def description: String = identifier
  def description_=(description: String) {}
  def content: Content = new Content(identifier)

  private def mergedSer(tarSer: TBaseSer) {
    val srcSerOpt = tarSer.freq.unit match {
      case TUnit.Day | TUnit.Week | TUnit.Month | TUnit.Year => serOf(TFreq.DAILY)
      case _ => serOf(TFreq.ONE_MIN)
    }

    srcSerOpt foreach { srcSer => TSerMerger.merge(srcSer.asInstanceOf[TBaseSer], tarSer, TimeZone.getDefault(), 0) }
  }
}

class ExampleSer(_thing: Thing, _freq: TFreq) extends DefaultTBaseSer(_thing, _freq) {

  val open = TVar[Double]("O", TVar.Kind.Open)
  val high = TVar[Double]("H", TVar.Kind.High)
  val low = TVar[Double]("L", TVar.Kind.Low)
  val close = TVar[Double]("C", TVar.Kind.Close)
  val volume = TVar[Double]("V", TVar.Kind.Accumlate)
  val isClosed = TVar[Boolean]("E")

  override val exportableVars = List(open, high, low, close, volume)

  reactions += updateBehavior

  private val tvalShift = new TValShift(freq, Calendar.getInstance())
  private var prevTVal: TVal = null
  def updateBehavior: Actor.Receive = {
    case x @ ExampleThing.Snapshot(_time, p, v) =>
      val tval = tvalShift.valOf(_time)
      val time = tval.time
      val alreadyExists = exists(time)

      if (!alreadyExists) {
        createOrReset(time)

        open(time) = p
        high(time) = p
        low(time) = p
        close(time) = p
        volume(time) = v

      } else {

        high(time) = math.max(p, high(time))
        low(time) = math.min(p, low(time))
        close(time) = p
        volume(time) = volume(time) + v

      }

      if (tval.justOpen_?) {
        tval.unjustOpen_!
      }

      if (prevTVal != null && prevTVal.closed_?) {
        isClosed(prevTVal.time) = true
      }

      prevTVal = tval

      publish(TSerEvent.Updated(this, "", time, time))
    case _ =>
  }
}