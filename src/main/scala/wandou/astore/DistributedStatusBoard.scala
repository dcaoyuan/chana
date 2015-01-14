package wandou.astore

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Address
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.cluster.Member
import akka.cluster.MemberStatus
import scala.collection.immutable
import scala.collection.immutable.TreeMap
import scala.concurrent.duration._
import scala.concurrent.forkjoin.ThreadLocalRandom

object DistributedStatusBoard {

  @SerialVersionUID(1L) case class Put[T](key: String, value: T)
  @SerialVersionUID(1L) case class Remove(key: String)
  @SerialVersionUID(1L) case class PutAck(key: String)
  @SerialVersionUID(1L) case class RemoveAck(key: String)

  // Only for testing purposes, to poll/await replication
  case object Count

  /**
   * INTERNAL API
   */
  private[astore] object Internal {
    case object Prune

    @SerialVersionUID(1L)
    case class Bucket[+T](
      owner: Address,
      version: Long,
      content: TreeMap[String, ValueHolder[T]])

    @SerialVersionUID(1L)
    case class ValueHolder[+T](version: Long, value: Option[T])

    @SerialVersionUID(1L)
    case class Status(versions: Map[Address, Long]) extends DistributedStatusBoardMessage
    @SerialVersionUID(1L)
    case class Delta[T](buckets: immutable.Iterable[Bucket[T]]) extends DistributedStatusBoardMessage

    case object GossipTick
  }
}

/**
 * Marker trait for remote messages with special serializer.
 */
trait DistributedStatusBoardMessage extends Serializable

/**
 * This actor manages a registry of actor references and replicates
 * the entries to peer actors among all cluster nodes or a group of nodes
 * tagged with a specific role.
 *
 * The `DistributedStatusBoard` is supposed to be started on all nodes,
 * or all nodes with specified role, in the cluster. The board can be
 * started with a [[DistributedXXXExtension]] or as an ordinary actor.
 *
 * Changes are only performed in the own part of the registry and those changes
 * are versioned. Deltas are disseminated in a scalable way to other nodes with
 * a gossip protocol. The registry is eventually consistent, i.e. changes are not
 * immediately visible at other nodes, but typically they will be fully replicated
 * to all other nodes after a few seconds.
 *
 * You can put status via the board on any node to registered actors on
 * any other node. There is three modes of message delivery.
 *
 * [[DistributedStatusBoard.Put]] -
 * The message will be delivered to all recipients with a matching path. Actors with
 * the same path, without address information, can be registered on different nodes.
 * On each node there can only be one such actor, since the path is unique within one
 * local actor system. Typical usage of this mode is to broadcast messages to all replicas
 * with the same path, e.g. 3 actors on different nodes that all perform the same actions,
 * for redundancy.
 *
 * You register status to the local board with [[DistributedStatusBoard.Put]]
 * The `ActorRef` in `Put` must belong to the same local actor system as the mediator.
 * Actors are automatically removed from the registry when they are terminated, or you
 * can explicitly remove entries with [[DistributedStatusBoard.Remove]]
 *  *
 * Successful `Put` and `Remove` is acknowledged with
 * [[DistributedStatusBoard.PutAck]] and [[DistributedStatusBoard.RemoveAck]]
 * replies.
 */
trait DistributedStatusBoard[T] extends Actor with ActorLogging {
  protected def role: Option[String]
  protected def gossipInterval: FiniteDuration
  protected def removedTimeToLive: FiniteDuration
  protected def maxDeltaElements: Int

  import DistributedStatusBoard._
  import DistributedStatusBoard.Internal._

  val cluster = Cluster(context.system)
  import cluster.selfAddress

  require(
    role.forall(cluster.selfRoles.contains),
    s"This cluster member [${selfAddress}] doesn't have the role [$role]")

  val removedTimeToLiveMillis = removedTimeToLive.toMillis

  //Start periodic gossip to random nodes in cluster
  import context.dispatcher
  val gossipTask = context.system.scheduler.schedule(gossipInterval, gossipInterval, self, GossipTick)
  val pruneInterval: FiniteDuration = removedTimeToLive / 2
  val pruneTask = context.system.scheduler.schedule(pruneInterval, pruneInterval, self, Prune)

  var registry: Map[Address, Bucket[T]] = Map.empty.withDefault(addr => Bucket(addr, 0L, TreeMap.empty))
  var nodes: Set[Address] = Set.empty

  // the version is a timestamp because it is also used when pruning removed entries
  val nextVersion = {
    var version = 0L
    () => {
      val current = System.currentTimeMillis
      version = if (current > version) current else version + 1
      version
    }
  }

  override def preStart(): Unit = {
    super.preStart()
    require(!cluster.isTerminated, "Cluster node must not be terminated")
    cluster.subscribe(self, classOf[MemberEvent])
  }

  override def postStop(): Unit = {
    super.postStop()
    cluster unsubscribe self
    gossipTask.cancel()
    pruneTask.cancel()
  }

  def matchingRole(m: Member): Boolean = role.forall(m.hasRole)

  def generalReceive: Receive = {
    case x: Put[T] =>
      put(x.key, Some(x.value))
      sender() ! PutAck(x.key)

    case Remove(key) =>
      registry(selfAddress).content.get(key) match {
        case Some(ValueHolder(_, Some(value))) =>
          put(key, None)
        case _ =>
      }
      sender() ! RemoveAck(key)

    case Status(otherVersions) =>
      // gossip chat starts with a Status message, containing the bucket versions of the other node
      val delta = collectDelta(otherVersions)
      if (delta.nonEmpty)
        sender() ! Delta(delta)
      if (otherHasNewerVersions(otherVersions))
        sender() ! Status(versions = myVersions) // it will reply with Delta

    case x: Delta[T] =>
      // reply from Status message in the gossip chat
      // the Delta contains potential updates (newer versions) from the other node
      // only accept deltas/buckets from known nodes, otherwise there is a risk of
      // adding back entries when nodes are removed
      if (nodes(sender().path.address)) {
        updateDelta(x.buckets)
      }

    case GossipTick => gossip()

    case Prune      => prune()

    case state: CurrentClusterState =>
      nodes = state.members.collect {
        case m if m.status != MemberStatus.Joining && matchingRole(m) => m.address
      }

    case MemberUp(m) =>
      if (matchingRole(m))
        nodes += m.address

    case MemberRemoved(m, _) =>
      if (m.address == selfAddress)
        context stop self
      else if (matchingRole(m)) {
        nodes -= m.address
        registry -= m.address
      }

    case _: MemberEvent => // not of interest

    case Count =>
      val count = registry.map {
        case (owner, bucket) => bucket.content.count {
          case (_, valueHolder) => valueHolder.value.isDefined
        }
      }.sum
      sender() ! count
  }

  def myVersions: Map[Address, Long] = registry.map { case (owner, bucket) => (owner -> bucket.version) }

  def otherHasNewerVersions(otherVersions: Map[Address, Long]): Boolean =
    otherVersions.exists {
      case (owner, v) => v > registry(owner).version
    }

  /**
   * Gossip to peer nodes.
   */
  def gossip(): Unit = selectRandomNode((nodes - selfAddress).toVector) foreach gossipTo

  def gossipTo(address: Address): Unit = {
    context.actorSelection(self.path.toStringWithAddress(address)) ! Status(versions = myVersions)
  }

  def selectRandomNode(addresses: immutable.IndexedSeq[Address]): Option[Address] =
    if (addresses.isEmpty) None else Some(addresses(ThreadLocalRandom.current nextInt addresses.size))

  def collectDelta(otherVersions: Map[Address, Long]): immutable.Iterable[Bucket[T]] = {
    // missing entries are represented by version 0
    val filledOtherVersions = myVersions.map { case (k, _) => k -> 0L } ++ otherVersions
    var count = 0
    filledOtherVersions.collect {
      case (owner, v) if registry(owner).version > v && count < maxDeltaElements =>
        val bucket = registry(owner)
        val deltaContent = bucket.content.filter {
          case (_, value) => value.version > v
        }
        count += deltaContent.size
        if (count <= maxDeltaElements)
          bucket.copy(content = deltaContent)
        else {
          // exceeded the maxDeltaElements, pick the elements with lowest versions
          val sortedContent = deltaContent.toVector.sortBy(_._2.version)
          val chunk = sortedContent.take(maxDeltaElements - (count - sortedContent.size))
          bucket.copy(
            version = chunk.last._2.version,
            content = TreeMap.empty[String, ValueHolder[T]] ++ chunk)
        }
    }
  }

  def put(key: String, valueOption: Option[T]): Unit = {
    val bucket = registry(selfAddress)
    val v = nextVersion()
    registry += (selfAddress -> bucket.copy(
      version = v,
      content = bucket.content + (key -> ValueHolder(v, valueOption))))
    onPut(Set(key))
  }

  def updateDelta(buckets: immutable.Iterable[Bucket[T]]): Unit = {
    var updatedKeys = Set[String]()
    buckets.foreach {
      case b: Bucket[T] =>
        if (nodes(b.owner)) {
          val myBucket = registry(b.owner)
          if (b.version > myBucket.version) {
            registry += (b.owner -> myBucket.copy(
              version = b.version,
              content = myBucket.content ++ b.content))
            updatedKeys ++= b.content.keys
          }
        }
    }
    onPut(updatedKeys)
  }

  def prune(): Unit = {
    registry foreach {
      case (owner, bucket) =>
        val oldRemoved = bucket.content.collect {
          // TODO when there is only one node, since the bucker.version could be the same as version,
          // 'if (bucket.version - version > removedTimeToLiveMillis)' may be always false?
          case (key, ValueHolder(version, None)) /* if (bucket.version - version > removedTimeToLiveMillis) */ => key
        }
        if (oldRemoved.nonEmpty) {
          registry += owner -> bucket.copy(content = bucket.content -- oldRemoved)
          onRemoved(oldRemoved.toSet)
        }
    }
  }

  def onPut(keys: Set[String]) {}
  def onRemoved(keys: Set[String]) {}
}

