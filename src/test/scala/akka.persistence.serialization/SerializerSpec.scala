package akka.persistence.serialization

import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }
import akka.actor.ActorSystem
import akka.testkit.{ ImplicitSender, TestKit }
import akka.persistence.PersistentRepr
import akka.persistence.journal.AsyncWriteTarget.WriteMessages
import akka.serialization.SerializationExtension
import com.typesafe.config.ConfigFactory
import scala.collection.immutable

class SerializerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("MySpec", ConfigFactory.parseString(wandou.astore.serializer.SerializerData.testConfig)))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  val serialization = SerializationExtension(system)

  def test(obj: AnyRef) = {
    val serializer = serialization.findSerializerFor(obj)
    val bytes = serializer.toBinary(obj)
    val res = serialization.deserialize(bytes, obj.getClass).get
    assertResult(obj)(res)

    val resById = serialization.deserialize(bytes, serializer.identifier, Some(obj.getClass)).get
    assertResult(obj)(resById)
  }

  "Serializer" must {
    "handle Avro Record" in {
      test(SerializerData.writeMessages)
    }
  }
}

object SerializerData {
  val schema = wandou.astore.serializer.SerializerData.schema
  val record = wandou.astore.serializer.SerializerData.record
  val repr = PersistentRepr(record)
  val writeMessages = WriteMessages(List(repr))
}