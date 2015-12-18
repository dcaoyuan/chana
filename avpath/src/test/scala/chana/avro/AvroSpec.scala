package chana.avro

import chana.avro.test.ChargeRecord
import org.apache.avro.generic.GenericData
import org.scalatest.{ Matchers, BeforeAndAfterAll, WordSpecLike }

class AvroSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {

  "Avro record builder" when {
    import AvroRecords._

    val jsonAccountDefault = """
{"id": "-1", "registerTime": 0, "lastLoginTime": 0, "loginRecords": [], "chargeRecords": [], "activityRecords": [], "balance": 0.0, "numFriends": 0, "numContacts": 0, "numPlayedGames": 0, "score": 0.0, "scoreActives": 0.0, "scoreFaithful": 0.0, "scoreLogins": 0.0, "scorePayments": 0.0, "scoreApps": 0.0, "scoreActions": 0.0, "devApps": {}, "devActions": {}}
""".trim

    val jsonAccountUncomplete = """
{"registerTime":1,"lastLoginTime":1}
""".trim
    val jsonAccountFilled = """
{"id": "-1", "registerTime": 1, "lastLoginTime": 1, "loginRecords": [], "chargeRecords": [], "activityRecords": [], "balance": 0.0, "numFriends": 0, "numContacts": 0, "numPlayedGames": 0, "score": 0.0, "scoreActives": 0.0, "scoreFaithful": 0.0, "scoreLogins": 0.0, "scorePayments": 0.0, "scoreApps": 0.0, "scoreActions": 0.0, "devApps": {}, "devActions": {}}
""".trim

    val jsonAccountReal = """
{"id":"12","registerTime":687557347200000,"lastLoginTime":688421347200000,"loginRecords":[{"time":688421347200000,"kind":"WDJ"},{"time":1401248164983,"kind":"GAME"},{"time":1401248164993,"kind":"GAME"}],"chargeRecords":[{"time":-17188358400000,"amount":20.0}],"activityRecords":null,"balance":100.0,"devApps":null,"devActions":{"udid121":{"actionRecords":[{"time":1401248186700,"kind":"DOWNLOAD"},{"time":1401248186700,"kind":"CLICK"},{"time":1401248186700,"kind":"QUERY"}]},"udid122":{"actionRecords":[{"time":1401248186701,"kind":"CLICK"},{"time":1401248186701,"kind":"QUERY"}]}}}
""".trim

    s"using GenericRecordBuilder to create new record:" should {
      val account = accountBuilder.build()
      info("Account: \n" + account)
      val fromJsonAccountDefault = FromJson.fromJsonString(jsonAccountDefault, schema)
      s"be filled with default value as:\n ${fromJsonAccountDefault}" in {
        account should be(fromJsonAccountDefault)
      }
    }

    s"an uncomplete json:\n ${jsonAccountUncomplete} \n its uncompleted fields" should {
      val fromJsonAccount1 = FromJson.fromJsonString(jsonAccountUncomplete, schema).asInstanceOf[GenericData.Record]
      val fromJsonAccountFilled1 = FromJson.fromJsonString(jsonAccountFilled, schema)
      s"be filled with default value by FromJson.fromJsonString (generic) as:\n ${fromJsonAccountFilled1}" in {
        fromJsonAccount1 should be(fromJsonAccountFilled1)
      }

      val fromJsonAccount2 = FromJson.fromJsonString(jsonAccountUncomplete, schema, true)
      val fromJsonAccountFilled2 = FromJson.fromJsonString(jsonAccountFilled, schema, true)
      s"be filled with default value by FromJson.fromJsonString (specified) as:\n ${fromJsonAccountFilled2}" in {
        fromJsonAccount2 should be(fromJsonAccountFilled2)
      }

      val jsonAccountUncomplete1 = ToJson.toJsonString(fromJsonAccount1, schema)
      s"be compacted to concise json string which drops all default values vice versa as:\n ${jsonAccountUncomplete1}" in {
        jsonAccountUncomplete1 should be(jsonAccountUncomplete)
      }
    }

    s"a real world json:\n ${jsonAccountReal} \n " should {
      val fromJsonAccount1 = FromJson.fromJsonString(jsonAccountReal, schema)
      val fromJsonAccountReal1 = FromJson.fromJsonString(ToJson.toJsonString(fromJsonAccount1, schema), schema)
      s"be exactly the same as the one decoded from FromJson.fromJsonString (generic) as:\n ${fromJsonAccountReal1}" in {
        fromJsonAccountReal1 should be(fromJsonAccount1)
      }

      val fromJsonAccount2 = FromJson.fromJsonString(jsonAccountReal, schema, true)
      val fromJspnAccountReal2 = FromJson.fromJsonString(ToJson.toJsonString(fromJsonAccount2, schema), schema, true)
      s"be exactly the same as the one decoded from FromJson.fromJsonString (specified)as:\n ${fromJspnAccountReal2}" in {
        fromJspnAccountReal2 should be(fromJsonAccount2)
      }
    }

  }

  "An Avro Encode/Decode" when {

    val schema = Schemas.account
    val chargeRecordSchema = schema.getField(Schemas.CHARGE_RECORDS).schema.getElementType
    val uid = 1L
    val time1 = System.currentTimeMillis
    val time2 = System.currentTimeMillis

    val chargeRecord1 = new GenericData.Record(chargeRecordSchema)
    chargeRecord1.put(Schemas.TIME, time1)
    chargeRecord1.put(Schemas.AMOUNT, 100.0)
    val chargeRecord2 = new GenericData.Record(chargeRecordSchema)
    chargeRecord2.put(Schemas.TIME, time1)
    chargeRecord2.put(Schemas.AMOUNT, -50.0)

    val chargeRecordsSchema = schema.getField(Schemas.CHARGE_RECORDS).schema
    val chargeRecords = new GenericData.Array[GenericData.Record](0, chargeRecordsSchema)
    chargeRecords.add(chargeRecord1)
    chargeRecords.add(chargeRecord2)

    val avroEncoded = avroEncode(chargeRecords, chargeRecordsSchema).get
    // decode to Generic records
    val avroDecoded = avroDecode[GenericData.Array[GenericData.Record]](avroEncoded, chargeRecordsSchema).get
    // decode to GhargeRecords
    val avroDecodedSpecified = avroDecode[GenericData.Array[ChargeRecord]](avroEncoded, chargeRecordsSchema, true).get

    // encode ChargeRecords
    val avroEncodedSpecified1 = avroEncode(avroDecodedSpecified, chargeRecordsSchema).get
    // decode to ChargeRecords
    val avroDecodedSpecified1 = avroDecode[GenericData.Array[ChargeRecord]](avroEncodedSpecified1, chargeRecordsSchema, true).get
    // decode to Generic records
    val avroDecodedGeneric1 = avroDecode[GenericData.Array[GenericData.Record]](avroEncodedSpecified1, chargeRecordsSchema, false).get

    val jsonEncoded = ToJson.toJsonString(chargeRecords, chargeRecordsSchema)
    val jsonDecoded = FromJson.fromJsonString(jsonEncoded, chargeRecordsSchema)

    s"avroEncode GenericRecords in length: ${avroEncoded.length}" should {
      "avroDecode to GenericRecords" in {
        avroDecoded should be(chargeRecords)
      }
    }

    s"avroEncode GenericRecords in length: ${avroEncoded.length}" should {
      "avroDecode to ChargeRecords" in {
        avroDecodedSpecified.get(0).isInstanceOf[ChargeRecord] should be(true)
      }
    }

    s"avroEncode ChargeRecords of number: ${avroDecodedSpecified.size}" should {
      "avroDecode to ChargeRecords" in {
        avroDecodedSpecified1 should be(avroDecodedSpecified)
      }
    }

    s"avroEncode ChargeRecords of number: ${avroDecodedSpecified.size}" should {
      "avroDecode to Generic records" in {
        avroDecodedGeneric1 should be(chargeRecords)
      }
    }

    s"jsonEncode GenericRecords in length: ${jsonEncoded.length}, ${new String(jsonEncoded)}" should {
      "jsonDecode to records" in {
        jsonDecoded should be(chargeRecords)
      }
    }

  }
}
