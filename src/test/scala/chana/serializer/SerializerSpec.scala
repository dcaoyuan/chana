package chana.serializer

import akka.actor.ActorSystem
import akka.testkit.{ ImplicitSender, TestKit }
import akka.serialization.SerializationExtension
import akka.util.ByteString
import chana.PutSchema
import chana.UpdatedFields
import chana.avro
import chana.avro.DefaultRecordBuilder
import chana.avro.UpdateEvent
import chana.jpql.BinaryProjection
import chana.jpql.RemoveProjection
import com.typesafe.config.ConfigFactory
import java.util.concurrent.TimeUnit
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }
import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration

object SerializerSpec {
  val config = ConfigFactory.parseString("""

akka.actor {
  serializers {
    avro = "chana.serializer.AvroSerializer"
    avro-projection = "chana.serializer.AvroProjectionSerializer"
    binlog = "chana.serializer.BinlogSerializer"
    java-map = "chana.serializer.JavaMapSerializer"
    record-event= "chana.serializer.RecordEventSerializer"
    schema = "chana.serializer.SchemaSerializer"
    schema-event= "chana.serializer.SchemaEventSerializer"
    update-event = "chana.serializer.UpdateEventSerializer"
    writemessages = "akka.persistence.serialization.WriteMessagesSerializer"
  }
                                         
  serialization-bindings {
    "akka.persistence.journal.AsyncWriteTarget$WriteMessages" = writemessages
    "chana.package$UpdatedFields" = record-event
    "chana.avro.Binlog" = binlog
    "chana.avro.UpdateEvent" = update-event
    "chana.jpql.AvroProjection" = avro-projection
    "chana.package$PutSchema" = schema-event
    "java.util.HashMap" = java-map
    "org.apache.avro.generic.GenericContainer" = avro
    "org.apache.avro.Schema" = schema
  }

  provider = "akka.cluster.ClusterActorRefProvider"
}

akka.remote.netty.tcp.hostname = "127.0.0.1"
akka.remote.netty.tcp.port = 0 

""")

  private val classLoader = this.getClass.getClassLoader

  val schema = new Parser().parse(classLoader.getResourceAsStream("avsc/PersonInfo.avsc"))
  val builder = DefaultRecordBuilder(schema)
  val emails = {
    val xs = new GenericData.Array[Utf8](0, schema.getField("emails").schema)
    // Utf8 with same string is not equils to String, we use Utf8 for test spec 
    xs.add(new Utf8("abc@abc.com"))
    xs.add(new Utf8("def@abc.com"))
    xs
  }
  val record = {
    val rec = builder.build()
    rec.put("emails", emails)
    rec
  }
}

class SerializerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {
  import SerializerSpec._

  def this() = this(ActorSystem("MySpec", SerializerSpec.config))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  val serialization = SerializationExtension(system)
  def test(obj: AnyRef) = {
    val serializer = serialization.findSerializerFor(obj)
    info("use " + serializer.getClass.getName)
    val bytes = serializer.toBinary(obj)
    val res = serialization.deserialize(bytes, obj.getClass).get
    res should be(obj)

    val resById = serialization.deserialize(bytes, serializer.identifier, Some(obj.getClass)).get
    resById should be(obj)
  }

  "StringSerializer" must {
    "handle string" in {
      val obj = "abcd中文"
      val builder = ByteString.newBuilder

      StringSerializer.appendToByteString(builder, obj)
      val bytes = builder.result.iterator
      StringSerializer.fromByteIterator(bytes) should be(obj)
    }

    "handle empty string" in {
      val obj = ""
      val builder = ByteString.newBuilder

      StringSerializer.appendToByteString(builder, obj)
      val bytes = builder.result.iterator
      StringSerializer.fromByteIterator(bytes) should be(obj)
    }
  }

  "Serializer" must {
    "handle Avro record" in {
      test(record)
    }

    "handle Avro array" in {
      test(emails)
    }

    "handle Avro schema" in {
      test(schema)
    }

    "handle PutSchema" in {
      val duration = FiniteDuration(100L, TimeUnit.SECONDS)
      val putSchema = PutSchema("entityName", schema.toString, None, duration)
      test(putSchema)
    }

    "handle UpdatedFields" in {
      val obj = UpdatedFields(List(
        (0, "James Bond"),
        (1, 30),
        (3, emails)))

      test(obj)
    }

    "handle java.util.Map" in {
      val obj = new java.util.HashMap[String, GenericData.Record]()
      obj.put("key1", record)
      obj.put("key2", record)

      test(obj)
    }

    "handle avro mapper projection " in {
      val obj = BinaryProjection("1234", chana.avro.avroEncode(record, schema).get)
      val serializer = serialization.findSerializerFor(obj)
      info("use " + serializer.getClass.getName)
      val bytes = serializer.toBinary(obj)
      val res = serialization.deserialize(bytes, obj.getClass).get
      //info(res.projection.mkString(","))
      //info(obj.projection.mkString(","))
      res.id should be(obj.id)
      res.projection should be(obj.projection)
    }

    "handle avro void projection " in {
      val obj = RemoveProjection("5678")
      test(obj)
    }

    "handle binlog" in {
      val obj = avro.Changelog("", record, record.getSchema)
      test(obj)
    }

    "handle update event" in {
      val obj = UpdateEvent(Array(avro.Changelog("/", record, record.getSchema)))
      test(obj)
    }

  }
}
