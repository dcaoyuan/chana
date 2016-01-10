package akka.persistence.serialization

import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }
import akka.actor.ActorSystem
import akka.testkit.{ ImplicitSender, TestKit }
import akka.persistence.AtomicWrite
import akka.persistence.PersistentRepr
import akka.persistence.journal.AsyncWriteTarget.WriteMessages
import akka.serialization.SerializationExtension
import scala.collection.immutable

object SerializerSpec {
  val schema = chana.serializer.SerializerSpec.schema
  val record = chana.serializer.SerializerSpec.record
  val atom = AtomicWrite(PersistentRepr(record))
  val writeMessages = WriteMessages(List(atom))
}

class SerializerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("SerializerSpec", chana.serializer.SerializerSpec.config))

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
      test(SerializerSpec.writeMessages)
    }
  }
}
