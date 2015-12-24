package chana.avro

import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecordBuilder

object AvroRecords {
  val schema = Schemas.account
  val chargeRecordsSchema = schema.getField(Schemas.CHARGE_RECORDS).schema
  val chargeRecordSchema = Schemas.chargeRecord
  val appInfoSchema = Schemas.appInfo

  val accountBuilder = new GenericRecordBuilder(schema)
  val chargeRecordBuilder = new GenericRecordBuilder(chargeRecordSchema)
  val appInfoBuilder = new GenericRecordBuilder(appInfoSchema)

  def initAccount() = {
    val account = accountBuilder.build()

    val uid = 1L
    val time1 = 1L
    val time2 = 2L

    val chargeRecord1 = chargeRecordBuilder.build()
    chargeRecord1.put(Schemas.TIME, time1)
    chargeRecord1.put(Schemas.AMOUNT, 100.0)
    val chargeRecord2 = chargeRecordBuilder.build()
    chargeRecord2.put(Schemas.TIME, time2)
    chargeRecord2.put(Schemas.AMOUNT, -50.0)

    val chargeRecords = new GenericData.Array[GenericData.Record](0, chargeRecordsSchema)
    chargeRecords.add(chargeRecord1)
    chargeRecords.add(chargeRecord2)

    val devApps = new java.util.HashMap[String, Any]()
    val appInfo1 = appInfoBuilder.build()
    val appInfo2 = appInfoBuilder.build()
    val appInfo3 = appInfoBuilder.build()
    appInfo1.put("numBlackApps", 1)
    appInfo2.put("numBlackApps", 2)
    appInfo3.put("numBlackApps", 3)
    devApps.put("a", appInfo1)
    devApps.put("b", appInfo2)
    devApps.put("c", appInfo3)

    account.put("chargeRecords", chargeRecords)
    account.put("lastChargeRecord", chargeRecord2)
    account.put("devApps", devApps)

    account.put("id", "abCd")
    account.put("registerTime", 10000L)
    account.put("lastLoginTime", 20000L)
    account.put("balance", 100.0)
    account
  }

}
