package chana 

import java.io.File

import akka.actor.{ActorIdentity, Identify, _}
import akka.cluster.{Cluster, MemberStatus}
import akka.contrib.datareplication.{DataReplication, LWWMap}
import akka.io.{IO, Tcp}
import akka.persistence.Persistence
import akka.persistence.journal.leveldb.{SharedLeveldbJournal, SharedLeveldbStore}
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.ImplicitSender
import chana.script.DistributedScriptBoard
import com.typesafe.config.ConfigFactory
import org.iq80.leveldb.util.FileUtils
import scala.concurrent.duration._
import spray.can.Http
import spray.http.HttpHeaders.RawHeader
import spray.http._
import spray.routing.Directives


/**
 * Note:
 * 1. cluster singleton proxy or cluster sharding proxy does not need to contain the corresponding role. 
 *
 * But
 *
 * 2. Start sharding or its proxy will try to create sharding coordinate singleton on the oldest node, 
 *    so the oldest node has to contain those (singleton, sharding) corresponding roles and start these
 *    sharding/singleton entry or proxy. 
 * 3. If the sharding coordinate is not be created/located in cluster yet, the sharding proxy in other node
 *    could not identify the coordinate singleton, which means, if you want to a sharding proxy to work 
 *    properly and which has no corresponding role contained, you have to wait for the coordinate singleton
 *    is ready in cluster.
 *
 * The sharding's singleton coordinator will be created and located at the oldest node.

 * Anyway, to free the nodes starting order, the first started node (oldest) should start all sharding 
 * sevices (or proxy) and singleton manager (or proxy) and thus has to contain all those corresponding roles,
 */
object ChanaClusterSpecConfig extends MultiNodeConfig {
  // first node is a special node for test spec
  val controller = role("controller")

  val entity1 = role("entity1")
  val entity2 = role("entity2")

  val client1 = role("client1")

  commonConfig(ConfigFactory.parseString(
    """
      akka.loglevel = INFO
      akka.log-config-on-start = off
      akka.actor {
        serializers {
          avro = "chana.serializer.AvroSerializer"
          schema = "chana.serializer.SchemaSerializer"
          record-event= "chana.serializer.RecordEventSerializer"
          schema-event= "chana.serializer.SchemaEventSerializer"
          writemessages = "akka.persistence.serialization.WriteMessagesSerializer"
        }
        serialization-bindings {
          "org.apache.avro.generic.GenericContainer" = avro
          "org.apache.avro.Schema" = schema
          "chana.package$UpdatedFields" = record-event
          "chana.package$PutSchema" = schema-event
          "akka.persistence.journal.AsyncWriteTarget$WriteMessages" = writemessages
        }
      }
      akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
      akka.extensions = ["akka.contrib.pattern.ClusterReceptionistExtension"]
      akka.persistence.journal.plugin = "akka.persistence.journal.leveldb-shared"
      akka.persistence.journal.leveldb-shared.store.native = off
      akka.persistence.journal.leveldb-shared.store.dir = "target/test-shared-journal"
      akka.persistence.snapshot-store.local.dir = "target/test-snapshots"
      chana.persistence.persistent = on
    """))

  nodeConfig(entity1) {
    ConfigFactory.parseString(
      """
        akka.extensions = [
          "chana.schema.DistributedSchemaBoard",
          "chana.script.DistributedScriptBoard"
        ]

        akka.contrib.cluster.sharding.role = "entity"
        akka.cluster.roles = ["entity"]

        akka.remote.netty.tcp.port = 2551
        web.port = 8081
      """)
  }

  nodeConfig(entity2) {
    ConfigFactory.parseString(
      """
        akka.extensions = [
          "chana.schema.DistributedSchemaBoard",
          "chana.script.DistributedScriptBoard"
        ]

        akka.contrib.cluster.sharding.role = "entity"
        akka.cluster.roles = ["entity"]

        akka.remote.netty.tcp.port = 2552
        web.port = 8082
      """)
  }


}

class ChanaClusterSpecMultiJvmNode1 extends ChanaClusterSpec
class ChanaClusterSpecMultiJvmNode2 extends ChanaClusterSpec
class ChanaClusterSpecMultiJvmNode3 extends ChanaClusterSpec
class ChanaClusterSpecMultiJvmNode4 extends ChanaClusterSpec

object ChanaClusterSpec {
  val avsc = """
{
  "type": "record",
  "name": "PersonInfo",
  "namespace": "chana",
  "fields": [
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "age",
      "type": "int"
    },
    {
      "name": "gender",
      "type": {
        "type": "enum",
        "name": "GenderType",
        "symbols": [
          "Female",
          "Male",
          "Unknown"
        ]
      },
      "default": "Unknown"
    },
    {
      "name": "emails",
      "type": {
        "type": "array",
        "items": "string"
      }
    }
  ]
}
  """
} 

class ChanaClusterSpec extends MultiNodeSpec(ChanaClusterSpecConfig) with STMultiNodeSpec with ImplicitSender {

  import chana.ChanaClusterSpec._
  import chana.ChanaClusterSpecConfig._

  override def initialParticipants: Int = roles.size

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      Cluster(system) join node(to).address
    }
  }

  val storageLocations = List(
    "akka.persistence.journal.leveldb.dir",
    "akka.persistence.journal.leveldb-shared.store.dir",
    "akka.persistence.snapshot-store.local.dir").map(s => new File(system.settings.config.getString(s)))

  override protected def atStartup() {
    runOn(controller) {
      storageLocations.foreach(dir => FileUtils.deleteRecursively(dir))
    }
  }

  override protected def afterTermination() {
    runOn(controller) {
      storageLocations.foreach(dir => FileUtils.deleteRecursively(dir))
    }
  }

  "Sharded chana cluster" must {

    "setup shared journal" in {
      // start the Persistence extension
      Persistence(system)
      runOn(controller) {
        system.actorOf(Props[SharedLeveldbStore], "store")
      }
      enterBarrier("peristence-started")

      runOn(entity1, entity2) {
        system.actorSelection(node(controller) / "user" / "store") ! Identify(None)
        val sharedStore = expectMsgType[ActorIdentity].ref.get
        SharedLeveldbJournal.setStore(sharedStore, system)
      }
      enterBarrier("setup-persistence")
    }

    "start cluster" in within(30.seconds) {

      val cluster = Cluster(system)

      join(entity1, entity1)
      join(entity2, entity1)

      // with roles: entity
      runOn(entity1, entity2) { 

        val routes = Directives.respondWithHeader(RawHeader("Access-Control-Allow-Origin", "*")) {
          new ChanaRoute(system).route
        }

        val server = system.actorOf(RestServer.props(routes), "chana-web")
        val webConfig = system.settings.config.getConfig("web")
        IO(Http) ! Http.Bind(server, "127.0.0.1", webConfig.getInt("port"))
        expectMsgType[Tcp.Bound]
      }

     runOn(entity1, entity2) {
        awaitAssert {
          self ! cluster.state.members.filter(_.status == MemberStatus.Up).size  
          expectMsg(2)
        }
        enterBarrier("start-cluster")
      }
      
      runOn(controller, client1) {
        enterBarrier("start-cluster")
      }

    }

    val baseUrl1 = "http://localhost:8081"
    val baseUrl2 = "http://localhost:8082"

    "do rest calling" in within(30.seconds) {
      enterBarrier("start-cluster")

      runOn(client1) {
        import spray.httpx.RequestBuilding._

        IO(Http) ! Get(baseUrl1 + "/ping")
        expectMsgType[HttpResponse](5.seconds).entity.asString should be("pong")

        IO(Http) ! Post(baseUrl1 + "/putschema/personinfo", avsc)
        expectMsgType[HttpResponse](5.seconds).entity.asString should be("OK")

        // Waiting for sharding singleton manager actor ready, which need to identify the oldest node. 
        // 
        // see https://groups.google.com/forum/#!topic/akka-user/_ZoOgeCE4bU
        // You were 5 seconds from seeing the expected result.
        // Increase the last sleep to 25 seconds.
        // You can reduce this timeout by using the following parameter to ClusterSingletonManager.props:
        //   maxHandOverRetries = 10,
        //   maxTakeOverRetries = 5
        // The default timeouts for the retries are too conservative in 2.2.3 (residue from first impl, which did not run the singleton on the oldest node).
        // In 2.3-M2 the default values for these settings have been reduced.
        expectNoMsg(12.seconds)

        IO(Http) ! Get(baseUrl1 + "/personinfo/get/1")
        expectMsgType[HttpResponse](5.seconds).entity.asString should be("""{"name":"","age":0,"gender":"Unknown","emails":[]}""")
        
        IO(Http) ! Get(baseUrl2 + "/personinfo/get/101")
        expectMsgType[HttpResponse](5.seconds).entity.asString should be("""{"name":"","age":0,"gender":"Unknown","emails":[]}""")

        IO(Http) ! Post(baseUrl1 + "/personinfo/update/1", ".\n{'name':'James Bond','age':60}")
        expectMsgType[HttpResponse](5.seconds).entity.asString should be("OK")

        IO(Http) ! Get(baseUrl2 + "/personinfo/get/1")
        expectMsgType[HttpResponse](5.seconds).entity.asString should be("""{"name":"James Bond","age":60,"gender":"Unknown","emails":[]}""")

        IO(Http) ! Get(baseUrl1 + "/personinfo/get/1/age")
        expectMsgType[HttpResponse](5.seconds).entity.asString should be("60")
        IO(Http) ! Get(baseUrl2 + "/personinfo/get/1/age")
        expectMsgType[HttpResponse](5.seconds).entity.asString should be("60")

        IO(Http) ! Post(baseUrl1 + "/personinfo/select/1", ".name")
        expectMsgType[HttpResponse](5.seconds).entity.asString should be("[\"James Bond\"]")

      }

      enterBarrier("rest-called")
    }

    "do script on updated" in within(30.seconds) {
      enterBarrier("rest-called")

      runOn(client1) {
        import spray.httpx.RequestBuilding._

        val script = 
  """
    function calc() {
        var a = record.get("age");
        print_me(a);
        print_me(http_get);
        var http_get_result = http_get.apply("http://localhost:8081/ping");
        print_me(http_get_result);
        java.lang.Thread.sleep(1000);
        print_me(http_get_result.value());

        var http_post_result = http_post.apply("http://localhost:8081/personinfo/put/2/age", "888");
        print_me(http_post_result);
        java.lang.Thread.sleep(1000);
        print_me(http_post_result.value());

        for (i = 0; i < fields.length; i++) {
            var field = fields[i];
            print_me(field._1);
            print_me(field._2);
        }
    }

    function print_me(value) {
        print(id + ":" + value);
    }

    calc();
    print("Current thread id: " + java.lang.Thread.currentThread().getId())
  """

        IO(Http) ! Post(baseUrl1 + "/personinfo/putscript/name/SCRIPT_NO_1", script)
        expectMsgType[HttpResponse](5.seconds).entity.asString should be("OK")

        IO(Http) ! Post(baseUrl1 + "/personinfo/update/1", ".\n{'name':'James Not Bond','age':60}")
        expectMsgType[HttpResponse](5.seconds).entity.asString should be("OK")

        awaitAssert {
          IO(Http) ! Get(baseUrl1 + "/personinfo/get/1/name")
          expectMsgType[HttpResponse](5.seconds).entity.asString should be("\"James Not Bond\"")
        }

        // awaitAssert script's http_post.apply("http://localhost:8081/personinfo/put/2/age", "888"), which was triggered by name updated.
        awaitAssert {
          IO(Http) ! Get(baseUrl1 + "/personinfo/get/2/age")
          expectMsgType[HttpResponse](5.seconds).entity.asString should be("888")
        }
        awaitAssert {
          IO(Http) ! Get(baseUrl2 + "/personinfo/get/2/age")
          expectMsgType[HttpResponse](5.seconds).entity.asString should be("888")
        }

        IO(Http) ! Post(baseUrl2 + "/personinfo/put/1/age", "100")
        expectMsgType[HttpResponse](5.seconds).entity.asString should be("OK")

        IO(Http) ! Get(baseUrl1 + "/personinfo/delscript/name/SCRIPT_NO_1")
        expectMsgType[HttpResponse](5.seconds).entity.asString should be("OK")

        enterBarrier("script-done")
      }

      runOn(entity1, entity2) {
        enterBarrier("script-done")

        import akka.contrib.datareplication.Replicator._
        val replicator = DataReplication(system).replicator
        awaitAssert {
          replicator ! Get(DistributedScriptBoard.DataKey, ReadQuorum(3.seconds), None)
          expectMsgPF(5.seconds) {
            case GetSuccess(DistributedScriptBoard.DataKey, data: LWWMap[String] @unchecked, _) =>
              data.entries.values.toSet should be(Set())
          }
        }
      }

      runOn(controller) {
        enterBarrier("script-done")
      }

      enterBarrier("done")
    }


  }
}
