package chana.jpql

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.Stash
import akka.contrib.pattern.{ ClusterReceptionistExtension, ClusterSingletonManager, ClusterSingletonProxy }
import chana.jpql.nodes.SelectStatement
import chana.jpql.nodes.Statement
import java.time.LocalDate
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.util.Sorting

object ResultItemOrdering extends Ordering[WorkingSet] {
  def compare(x: WorkingSet, y: WorkingSet) = {
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

final case class ReducerProjection(projection: Record)

object JPQLReducer {
  def props(jpqlKey: String, stmt: Statement, projectionSchema: Schema): Props = Props(classOf[JPQLReducer], jpqlKey, stmt, projectionSchema)

  case object AskResult
  case object AskReducedResult

  val role = Some("jpql")

  def singletonManagerName(key: String) = "jpqlSingleton-" + key
  def reducerPath(key: String) = "/user/" + singletonManagerName(key) + "/" + key
  def reducerProxyName(key: String) = "jpqlProxy-" + key
  def reducerProxyPath(key: String) = "/user/" + reducerProxyName(key)

  def startReducer(system: ActorSystem, role: Option[String], jpqlKey: String, stmt: Statement, projectionSchema: Schema) = {
    system.actorOf(
      ClusterSingletonManager.props(
        singletonProps = props(jpqlKey, stmt, projectionSchema),
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

class JPQLReducer(jqplKey: String, stmt: Statement, projectionSchema: Schema) extends Actor with Stash with ActorLogging {

  import chana.jpql.JPQLReducer._
  import context.dispatcher

  log.info("JPQLReducer {} started", jqplKey)
  ClusterReceptionistExtension(context.system).registerService(self)

  private var idToProjection = Map[String, ReducerProjection]()
  private var prevUpdateTime: LocalDate = _
  private var today: LocalDate = _
  private val evaluator = new JPQLReducerEvaluator(log)
  private val withGroupby = stmt match {
    case x: SelectStatement => x.groupby.isDefined
    case _                  => false
  }

  override def preStart {
    prevUpdateTime = LocalDate.now()
  }

  override def postStop() {
    super.postStop()
  }

  def receive: Receive = {
    case VoidProjection(id) =>
      idToProjection -= id // remove
    case Projection(id, projectionBytes) =>
      chana.avro.avroDecode[Record](projectionBytes, projectionSchema) match {
        case Success(projection) => idToProjection += (id -> ReducerProjection(projection))
        case Failure(ex)         => log.warning("Failed to decode projection bytes: " + ex.getMessage)
      }

    case AskResult =>
      val commander = sender()
      commander ! idToProjection

    case AskReducedResult =>
      val commander = sender()
      commander ! reduce(idToProjection)

    case _ =>
  }

  def reduce(idToDataset: Map[String, ReducerProjection]): Array[List[Any]] = {
    if (idToDataset.isEmpty) {
      Array()
    } else {
      val datasets = idToDataset.values
      val reduced = if (withGroupby) {
        collectGroupbys(datasets).groupBy { _._1 }.map {
          case (groupKey, subDatasets) => reduceDataset(subDatasets.map { _._2 }).find { _ ne null }
        }.flatten.toArray
      } else {
        reduceDataset(datasets)
      }

      val isOrderby = reduced.headOption.fold(false) { _.orderbys.nonEmpty }
      if (isOrderby) {
        Sorting.quickSort(reduced)(ResultItemOrdering)
        log.debug("sorted by {} etc", reduced.head.orderbys)
      }

      val n = reduced.length
      val result = Array.ofDim[List[Any]](n)
      var i = 0
      while (i < n) {
        result(i) = reduced(i).selectedItems
        i += 1
      }
      result
    }
  }

  def collectGroupbys(datasets: Iterable[ReducerProjection]) = {
    evaluator.reset(datasets)
    datasets.map { dataset => (evaluator.visitGroupbys(stmt, dataset.projection), dataset) }
  }

  def reduceDataset(datasets: Iterable[ReducerProjection]): Array[WorkingSet] = {
    evaluator.reset(datasets)
    val n = datasets.size
    val reduced = Array.ofDim[WorkingSet](n)
    var i = 0
    val itr = datasets.iterator
    while (itr.hasNext) {
      val entry = itr.next
      reduced(i) = evaluator.visitOneRecord(stmt, entry.projection)
      i += 1
    }
    log.debug("reduced: {}", reduced.mkString("Array(", ",", ")"))

    reduced
  }
}