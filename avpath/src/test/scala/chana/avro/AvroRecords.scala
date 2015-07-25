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

  val jsonAccountDefault = """
{"id": "-1", "registerTime": 0, "lastLoginTime": 0, "loginRecords": [], "chargeRecords": [], "activityRecords": [], "balance": 0.0, "numFriends": 0, "numContacts": 0, "numPlayedGames": 0, "score": 0.0, "scoreActives": 0.0, "scoreFaithful": 0.0, "scoreLogins": 0.0, "scorePayments": 0.0, "scoreApps": 0.0, "scoreActions": 0.0, "devApps": {}, "devActions": {}}
""".trim

  val jsonAccountUncomp = """
{"registerTime":1,"lastLoginTime":1}
""".trim
  val jsonAccountFilled = """
{"id": "-1", "registerTime": 1, "lastLoginTime": 1, "loginRecords": [], "chargeRecords": [], "activityRecords": [], "balance": 0.0, "numFriends": 0, "numContacts": 0, "numPlayedGames": 0, "score": 0.0, "scoreActives": 0.0, "scoreFaithful": 0.0, "scoreLogins": 0.0, "scorePayments": 0.0, "scoreApps": 0.0, "scoreActions": 0.0, "devApps": {}, "devActions": {}}
""".trim

  val jsonAccountReal = """
{"id":"12","registerTime":687557347200000,"lastLoginTime":688421347200000,"loginRecords":[{"time":688421347200000,"kind":"WDJ"},{"time":1401248164983,"kind":"GAME"},{"time":1401248164993,"kind":"GAME"}],"chargeRecords":[{"time":-17188358400000,"amount":20.0}],"activityRecords":null,"balance":100.0,"devApps":null,"devActions":{"udid121":{"actionRecords":[{"time":1401248186700,"kind":"DOWNLOAD"},{"time":1401248186700,"kind":"CLICK"},{"time":1401248186700,"kind":"QUERY"}]},"udid122":{"actionRecords":[{"time":1401248186701,"kind":"CLICK"},{"time":1401248186701,"kind":"QUERY"}]}}}
""".trim

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
    account.put("devApps", devApps)
    account
  }

}
