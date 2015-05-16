package chana.avro

import chana.avro.test.Account
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

class FromToJsonSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {
  val schema = Schemas.account

  "FromToJson" when {
    "FromtoJsonString" in {
      val account = new Account
      account.setId("1")
      account.setRegisterTime(System.currentTimeMillis())
      val jsonStr = ToJson.toJsonString(account, schema)
      assertResult(account.toString)(FromJson.fromJsonString(jsonStr, schema).toString)
    }
  }
}
