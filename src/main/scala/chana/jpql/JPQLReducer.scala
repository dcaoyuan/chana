package chana.jpql

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.Stash
import akka.contrib.pattern.{ ClusterReceptionistExtension, ClusterSingletonManager, ClusterSingletonProxy }
import chana.jpql.nodes.SelectStatement
import java.time.LocalDate
import org.apache.avro.generic.IndexedRecord
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.util.Sorting

object ResultItemOrdering extends Ordering[WorkSet] {
  def compare(x: WorkSet, y: WorkSet) = {
    var xs = x.orderbys
    var ys = y.orderbys

    var hint = 0
    while (hint == 0 && xs.nonEmpty) {
      hint = (xs.head, ys.head) match {
        case (a: Number, b: Number) => a.doubleValue.compareTo(b.doubleValue)
        case ((isAsc: Boolean, a: CharSequence), (_: Boolean, b: CharSequence)) =>
          (if (isAsc) 1 else -1) * a.toString.compareToIgnoreCase(b.toString)
        case _ => 0
      }
      xs = xs.tail
      ys = ys.tail
    }

    hint
  }
}

final case class RecordProjection(projection: IndexedRecord)

object JPQLReducer {
  def props(jpqlKey: String, meta: JPQLMeta): Props = Props(classOf[JPQLReducer], jpqlKey, meta)

  case object AskResult
  case object AskReducedResult

  val role = Some("chana-jpql")

  def singletonManagerName(key: String) = "jpqlSingleton-" + key
  def reducerPath(key: String) = "/user/" + singletonManagerName(key) + "/" + key
  def reducerProxyName(key: String) = "jpqlProxy-" + key
  def reducerProxyPath(key: String) = "/user/" + reducerProxyName(key)

  def startReducer(system: ActorSystem, role: Option[String], jpqlKey: String, meta: JPQLMeta) = {
    system.actorOf(
      ClusterSingletonManager.props(
        singletonProps = props(jpqlKey, meta),
        singletonName = jpqlKey,
        terminationMessage = PoisonPill,
        role = role),
      name = singletonManagerName(jpqlKey))
  }

  def startReducerProxy(system: ActorSystem, role: Option[String], jpqlKey: String) {
    val proxy = system.actorOf(
      ClusterSingletonProxy.props(singletonPath = reducerPath(jpqlKey), role = role),
      reducerProxyName(jpqlKey))
    ClusterReceptionistExtension(system).registerService(proxy)
  }

  def reducerProxy(system: ActorSystem, jpqlKey: String) = system.actorSelection(reducerProxyPath(jpqlKey))
}

class JPQLReducer(jqplKey: String, meta: JPQLSelect) extends Actor with Stash with ActorLogging {

  import chana.jpql.JPQLReducer._
  import context.dispatcher

  log.info("JPQLReducer {} started", jqplKey)
  ClusterReceptionistExtension(context.system).registerService(self)

  private var idToProjection = Map[String, RecordProjection]()
  private var prevUpdateTime: LocalDate = _
  private var today: LocalDate = _
  private val evaluator = new JPQLReducerEvaluator(meta, log)
  private val withGroupby = meta.stmt match {
    case x: SelectStatement => x.groupby.isDefined
    case _                  => false
  }

  private val isSelectItemsAllAggregate = meta.stmt.isSelectItemsAllAggregate

  override def preStart {
    prevUpdateTime = LocalDate.now()
  }

  override def postStop() {
    super.postStop()
  }

  def receive: Receive = {

    case BinaryProjection(id, bytes) =>
      chana.avro.avroDecode[IndexedRecord](bytes, meta.projectionSchema.head) match { // TODO multiple projectionSchema
        case Success(projection) => idToProjection += (id -> RecordProjection(projection))
        case Failure(ex)         => log.warning("Failed to decode projection bytes: " + ex.getMessage)
      }

    case RemoveProjection(id) =>
      idToProjection -= id // remove

    case DeletedRecord(id) =>
      idToProjection -= id // remove

    case AskResult =>
      val commander = sender()
      commander ! idToProjection

    case AskReducedResult =>
      val commander = sender()
      commander ! reduce(idToProjection)

    case _ =>
  }

  def reduce(idToDataset: Map[String, RecordProjection]): Array[List[Any]] = {
    if (idToDataset.isEmpty) {
      Array()
    } else {
      val reduced = if (withGroupby) {
        applyGroupbys(idToDataset).toArray
      } else {
        reduceDataset(idToDataset).toArray
      }

      if (reduced.length > 0) {
        val isOrderby = reduced(0).orderbys.nonEmpty
        if (isOrderby) {
          Sorting.quickSort(reduced)(ResultItemOrdering)
          log.debug("sorted by {}", reduced(0).orderbys)
        }

        val n = reduced.length
        val res = Array.ofDim[List[Any]](n)
        var i = 0

        while (i < n) {
          res(i) = reduced(i).selectedItems
          i += 1
        }
        res

      } else {
        Array()
      }
    }
  }

  def applyGroupbys(idToDataset: Map[String, RecordProjection]) = {
    evaluator.reset(idToDataset)
    val groupKeyToSubset = idToDataset.groupBy { case (id, dataset) => evaluator.visitGroupbys(id, dataset.projection) }
    groupKeyToSubset.map {
      case (groupKey, subset) => reduceDataset(subset).find { x => (x ne null) && x.having } // groupby should return only one result
    }.flatten
  }

  def reduceDataset(idToDataset: Map[String, RecordProjection]): List[WorkSet] = {
    evaluator.reset(idToDataset)
    var reduced = List[WorkSet]()
    val itr = idToDataset.iterator
    var break = false

    while (itr.hasNext && !break) {
      val (id, dataSet) = itr.next
      reduced :::= evaluator.visitOneRecord(id, dataSet.projection)
      if (isSelectItemsAllAggregate) {
        break = true
      }
    }
    log.debug("reduced: {}", reduced)

    reduced
  }
}