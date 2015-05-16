package chana.avro

import chana.avro.test.ChargeRecord
import org.apache.avro.generic.GenericData
import org.scalatest.{ Matchers, BeforeAndAfterAll, WordSpecLike }

class AvroSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {

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
        assertResult(chargeRecords)(avroDecoded)
      }
    }

    s"avroEncode GenericRecords in length: ${avroEncoded.length}" should {
      "avroDecode to ChargeRecords" in {
        assertResult(avroDecodedSpecified.get(0).isInstanceOf[ChargeRecord])(true)
      }
    }

    s"avroEncode ChargeRecords of number: ${avroDecodedSpecified.size}" should {
      "avroDecode to ChargeRecords" in {
        assertResult(avroDecodedSpecified)(avroDecodedSpecified1)
      }
    }

    s"avroEncode ChargeRecords of number: ${avroDecodedSpecified.size}" should {
      "avroDecode to Generic records" in {
        assertResult(chargeRecords)(avroDecodedGeneric1)
      }
    }

    s"jsonEncode GenericRecords in length: ${jsonEncoded.length}, ${new String(jsonEncoded)}" should {
      "jsonDecode to records" in {
        assertResult(chargeRecords)(jsonDecoded)
      }
    }

  }
}
